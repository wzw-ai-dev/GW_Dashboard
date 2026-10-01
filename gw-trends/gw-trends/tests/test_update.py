import json, sys, unittest
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import update  # noqa: E402

CFG = json.loads((ROOT / "config/topic.json").read_text())
C = [{"title": f"Missile story {i}", "url": f"https://x.test/{i}", "source": "S", "published": "2026-10-01T00:00:00+00:00", "snippet": ""} for i in range(5)]


def out(items, take="ok"):
    return json.dumps({"takeaway": take, "items": items})


def it(i, **kw):
    return {"url": f"https://x.test/{i}", "headline": "H", "why": "W", "category": "Technology", **kw}


class Tests(unittest.TestCase):
    def test_valid(self):
        r = update.validate_output(out([it(0), it(1), it(2)]), C, CFG)
        self.assertEqual([i["rank"] for i in r["items"]], [1, 2, 3])
        self.assertEqual(r["items"][0]["source"], "S")

    def test_invented_url_rejected(self):
        r = update.validate_output(out([it(0), it(1), it(2), {**it(3), "url": "https://fake.test/z"}]), C, CFG)
        self.assertEqual(len(r["items"]), 3)
        self.assertTrue(all(i["url"] in {c["url"] for c in C} for i in r["items"]))

    def test_all_invented_fails(self):
        with self.assertRaises(ValueError):
            update.validate_output(out([{**it(0), "url": "https://fake.test/a"}] * 4), C, CFG)

    def test_schema_errors(self):
        for bad in ("not json", "{}", out([it(0), it(1), it(2)], take="")):
            with self.assertRaises(Exception):
                update.validate_output(bad, C, CFG)

    def test_bad_category_becomes_other(self):
        r = update.validate_output(out([it(0, category="Zzz"), it(1), it(2)]), C, CFG)
        self.assertEqual(r["items"][0]["category"], "Other")

    def test_fenced_json_ok(self):
        update.validate_output("```json\n" + out([it(0), it(1), it(2)]) + "\n```", C, CFG)

    def test_dedupe(self):
        a = {"title": "Army tests new hypersonic missile", "url": "https://a.test/1?utm=1"}
        b = {"title": "Army tests new hypersonic missile!", "url": "https://b.test/2"}
        c = {"title": "Other", "url": "https://a.test/1"}
        self.assertEqual(len(update.dedupe([a, b, c])), 1)

    def test_parse_rss(self):
        xml = b"<rss><channel><item><title>Missile</title><link>https://a.test/1</link><pubDate>Thu, 01 Oct 2026 01:00:00 GMT</pubDate><description>&lt;p&gt;Snip&lt;/p&gt;</description></item></channel></rss>"
        r = update.parse_feed(xml, "S")
        self.assertEqual((r[0]["snippet"], r[0]["source"]), ("Snip", "S"))


if __name__ == "__main__":
    unittest.main()
