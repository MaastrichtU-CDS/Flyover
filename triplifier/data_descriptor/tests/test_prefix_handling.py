import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from annotation_helper.src.miscellaneous import get_unique_prefixes


class TestPrefixHandling(unittest.TestCase):
    def test_skips_duplicate_prefix_labels_with_different_spacing(self):
        query = "\n".join(
            [
                "PREFIX roo: <http://www.cancerdata.org/roo/>",
                "PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>",
            ]
        )
        prefixes = "\n".join(
            [
                "PREFIX roo:  <http://www.cancerdata.org/roo/>",
                "PREFIX ncit:  <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>",
                "PREFIX sio:  <http://semanticscience.org/resource/>",
            ]
        )

        result = get_unique_prefixes(query, prefixes)

        self.assertIn("PREFIX sio:  <http://semanticscience.org/resource/>", result)
        self.assertNotIn("PREFIX roo:  <http://www.cancerdata.org/roo/>", result)
        self.assertNotIn(
            "PREFIX ncit:  <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>",
            result,
        )


if __name__ == "__main__":
    unittest.main()
