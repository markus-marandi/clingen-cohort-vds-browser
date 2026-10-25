"""Synthetic pilot data only. No Hail, source cohort, network, or Elasticsearch."""
import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch


PATH = Path(__file__).resolve().parents[1] / "browser/data-pipeline/cohort_sample_export.py"
SPEC = importlib.util.spec_from_file_location("cohort_sample_export", PATH)
export = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(export)


class SampleExportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        files = {
            "samples": "sample_id\tsex_assigned\tdate_of_birth\tdate_seq\trun_id\tcare_site\tmaterial\thealth_status\n" + "".join(
                f"{row}\t2026-01-01\tSYN_RUN\tSYN_SITE\tblood\tAffected\n"
                for row in ("SYN_A\tUnknown\tprenatal", "SYN_B\tFemale\t12.03.2000",
                            "SYN_C\tMale\t2001-02-03", "SYN_D\tUnknown\tprenatal")
            ),
            "procedure": "sample_id\ttest\tinstrument\tref_genome\n" + "".join(f"SYN_{s}\tWES\tSynthetic\thg19\n" for s in "ABCD"),
            "panels": "sample_id\tpanel\nSYN_A\twhole exome\nSYN_B\tUnknown\nSYN_C\tCHOL\nSYN_C\tTEST_PANEL\n",
            "reports": "sample_id\tvariant_id\nSYN_A\tUnknown\nSYN_B\tNone\nSYN_C\t1-10-A-G\nSYN_C\t1-11-A-T\nSYN_D\t17-7807252--G-T\n",
        }
        for name, content in files.items():
            (self.root / name).write_text(content)

    def load(self):
        return export.load_samples(*(self.root / name for name in ("samples", "procedure", "panels", "reports")))

    def test_preserve_metadata_panels_and_report_states(self):
        samples, stats = self.load()
        self.assertEqual(samples["SYN_A"]["date_of_birth"], "prenatal")
        self.assertEqual(samples["SYN_B"]["date_of_birth"], "12.03.2000")
        self.assertEqual(samples["SYN_A"]["panels"], ["whole exome"])
        self.assertEqual(samples["SYN_B"]["panels"], ["Unknown"])
        self.assertEqual(samples["SYN_D"]["panels"], [])
        self.assertEqual(samples["SYN_C"]["panels"], ["CHOL", "TEST_PANEL"])
        self.assertEqual(samples["SYN_C"]["reported_variants"], ["1-10-A-G", "1-11-A-T"])
        self.assertEqual(samples["SYN_D"]["report_status"], "Reported")
        self.assertEqual(stats["omitted_report_entries"], 1)
        self.assertIsNone(export.report_flag(samples["SYN_A"], "1-10-A-G"))
        self.assertFalse(export.report_flag(samples["SYN_B"], "1-10-A-G"))
        self.assertTrue(export.report_flag(samples["SYN_C"], "1-10-A-G"))
        self.assertIsNone(export.report_flag(samples["SYN_D"], "1-10-A-G"))

    def test_carriers_use_called_alt_and_local_allele_depth(self):
        samples, stats = self.load()
        entry = {"sample_id": "SYN_C", "chrom": "1", "pos": 10, "alleles": ["A", "G", "T"],
                 "called_alleles": [0, 2], "genotype": "0/2", "lad": [12, 8], "local_alleles": [0, 2],
                 "depth": 20, "gq": 99, "gene_symbol": "TEST", "consequence": "missense_variant"}
        rows = list(export.carrier_documents([entry], samples, stats))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["variant_id"], "1-10-A-T")
        self.assertEqual(rows[0]["genotype"], "0/2")
        self.assertEqual(rows[0]["var_pct"], 0.4)
        self.assertEqual(stats["carrier_documents"], 1)
        entry.update(called_alleles=[0, 0], genotype="0/0")
        self.assertEqual(list(export.carrier_documents([entry], samples, Counter())), [])

    def test_zero_missing_depth_and_homozygous_alt(self):
        samples, stats = self.load()
        entry = {"sample_id": "SYN_B", "chrom": "1", "pos": 10, "alleles": ["A", "G"],
                 "called_alleles": [1, 1], "genotype": "1/1", "ad": [0, 0]}
        rows = list(export.carrier_documents([entry], samples, stats))
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["var_pct"])
        self.assertFalse(rows[0]["in_report"])

    def test_bulk_rejects_partial_failures(self):
        with patch.object(export, "request", return_value={"errors": True, "items": [{"index": {"status": 400}}]}):
            with self.assertRaisesRegex(RuntimeError, "incomplete"):
                export.bulk_write("http://unused.invalid", "samples", [{"sample_id": "SYN_A"}], sample_index=True)

    def test_bulk_batches_and_ids_are_stable(self):
        calls = []
        def request(method, url, body=None, content_type=None):
            calls.append((method, url, body))
            if url.endswith("_bulk"):
                lines = body.splitlines()
                return {"errors": False, "items": [{"index": {"status": 201}} for _ in lines[::2]]}
            return {}
        with patch.object(export, "request", side_effect=request):
            total = export.bulk_write("http://unused.invalid", "carriers", [
                {"sample_id": "SYN_A", "variant_id": f"1-{pos}-A-G"} for pos in (10, 11, 12)
            ], batch_size=2)
        self.assertEqual(total, 3)
        self.assertEqual([call[1] for call in calls], ["http://unused.invalid/_bulk"] * 2 + ["http://unused.invalid/carriers/_refresh"])
        self.assertEqual(json.loads(calls[0][2].splitlines()[0])["index"]["_id"], '["SYN_A","1-10-A-G"]')

    def test_dry_run_does_not_load_hail_or_contact_es(self):
        args = [str(PATH), "--sample-data", str(self.root / "samples"), "--procedure", str(self.root / "procedure"),
                "--panels", str(self.root / "panels"), "--in-report", str(self.root / "reports"), "--dry-run"]
        output = io.StringIO()
        with patch("sys.argv", args), patch.object(export, "request", side_effect=AssertionError("network")), patch.object(export, "mt_entries", side_effect=AssertionError("Hail")), contextlib.redirect_stdout(output):
            export.main()
        self.assertEqual(json.loads(output.getvalue())["samples"], 4)
        self.assertNotIn("SYN_A", output.getvalue())


if __name__ == "__main__":
    unittest.main()
