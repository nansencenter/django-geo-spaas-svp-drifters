import os
import sys
import argparse

os.environ['DJANGO_SETTINGS_MODULE'] = 'test_svp_drifters.settings'
sys.path.insert(0, 'test_svp_drifters')

import django
django.setup()

from django.test.utils import get_runner
from django.conf import settings


def runtests(moduleName=None):
    if moduleName is None:
        appPath = 'svp_drifters'
    else:
        appPath = 'svp_drifters.%s' % moduleName

    TestRunner = get_runner(settings)
    test_runner = TestRunner(verbosity=1, interactive=True)
    failures = test_runner.run_tests([appPath])
    sys.exit(failures)


if __name__ == '__main__':
    # get name of a module to test
    parser = argparse.ArgumentParser()
    parser.add_argument('module', type=str, nargs='?', default=None)
    args = parser.parse_args()
    runtests(args.module)
