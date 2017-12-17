from dateutil.parser import parse

from django.utils import timezone
from django.core.management.base import BaseCommand

from geospaas.utils import uris_from_args
from svp_drifters.models import SVPDrifter

class Command(BaseCommand):
    args = '''<metadata filename> <data filename>
              [--start <yyyy-mm-dd>]
              [--stop <yyyy-mm-dd>]
              [--maxnum number]
              [--minlon number]
              [--maxlon number]
              [--minlat number]
              [--maxlat number]'''
    help = 'Add drifter metadata to archive'

    def add_arguments(self, parser):
        parser.add_argument('--start',
                            action='store',
                            default='2014-01-01',
                            help='''Start''')
        parser.add_argument('--stop',
                            action='store',
                            default='2014-01-02',
                            help='''Stop''')
        parser.add_argument('--minlat',
                            action='store',
                            default='-90',
                            help='''Minimum latitutde''')
        parser.add_argument('--maxlat',
                            action='store',
                            default='90',
                            help='''Maximum latitutde''')
        parser.add_argument('--minlon',
                            action='store',
                            default='-180',
                            help='''Minimum longitude''')
        parser.add_argument('--maxlon',
                            action='store',
                            default='180',
                            help='''Maximum longitude''')
        parser.add_argument('--maxnum',
                            action='store',
                            default='None',
                            help='''Maximum number of drifters''')

    def handle(self, *args, **options):
        if not len(args)>=2:
            raise IOError('Please provide two filenames')
        start = parse(options['start'])
        stop = parse(options['stop'])
        minlon = float(options['minlon'])
        maxlon = float(options['maxlon'])
        minlat = float(options['minlat'])
        maxlat = float(options['maxlat'])
        maxnum = eval(options['maxnum'])
        
        uris = uris_from_args(args[0], args[1])
        count = SVPDrifter.objects.get_or_create(uris[0], uris[1],
                time_coverage_start=start,
                time_coverage_end=stop,
                minlat=minlat,
                maxlat=maxlat,
                minlon=minlon,
                maxlon=maxlon,
                maxnum=maxnum)
        print 'Added %d new drifter datasets'%count

