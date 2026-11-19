import unittest
from pharma_demo.synthetic import generate
from pharma_demo.reference import materialize, validate


class SyntheticPipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = generate(n=200)
        cls.out = materialize(cls.data["batch_1"] + cls.data["batch_2"],
                              cls.data["hcps"], cls.data["products"])

    def test_expected_active_count(self):
        self.assertEqual(len(self.out["current"]), 200)
        self.assertEqual(len(self.out["active"]), 180)
        self.assertEqual(sum(m["rx_count"] for m in self.out["daily_metrics"]), 180)

    def test_duplicate_and_bad_records(self):
        self.assertEqual(self.out["duplicate_events"], 2)
        self.assertEqual(len(self.out["quarantine"]), 1)
        self.assertEqual(self.out["quarantine"][0]["reason"], "missing_dimension_key")

    def test_cdc_late_event_does_not_undo_delete(self):
        by_rx = {r["rx_id"]: r for r in self.out["current"]}
        self.assertEqual(by_rx["RX000001"]["event_seq"], 3)
        self.assertEqual(by_rx["RX000001"]["operation"], "D")

    def test_updated_event_selected(self):
        by_rx = {r["rx_id"]: r for r in self.out["current"]}
        self.assertEqual(by_rx["RX000051"]["event_seq"], 2)

    def test_replay_is_idempotent(self):
        source = self.data["batch_1"] + self.data["batch_2"]
        replay = materialize(source + source, self.data["hcps"], self.data["products"])
        self.assertEqual(replay["active"], self.out["active"])
        self.assertEqual(replay["daily_metrics"], self.out["daily_metrics"])

    def test_validation(self):
        self.assertEqual(validate({"event_id": "x", "rx_id": "y", "operation": "WRONG"}),
                         "unsupported_operation")


if __name__ == "__main__":
    unittest.main()
