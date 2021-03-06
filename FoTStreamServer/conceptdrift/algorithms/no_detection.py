"""
The Tornado Framework
By Ali Pesaranghader
University of Ottawa, Ontario, Canada
E-mail: apesaran -at- uottawa -dot- ca / alipsgh -at- gmail -dot- com
"""

from detector import SuperDetector


class NO_DETECTION(SuperDetector):
    """The No Drift Detection class."""

    def __init__(self):
        super().__init__()

    def run(self, pr):
        return False, False

    def reset(self):
        super().reset()

    def get_settings(self):
        return "NO DETECTION"
