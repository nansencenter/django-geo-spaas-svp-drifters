from dateutil.parser import parse
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
        parser.add_argument('data_file',
                            nargs=1,
                            type=str)
        parser.add_argument('metadata_file',
                            nargs=1,
                            type=str)
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
        if not options['data_file'] or not options['metadata_file']:
            raise IOError('Please provide two filenames')

        start = parse(options['start'])
        stop = parse(options['stop'])
        minlon = float(options['minlon'])
        maxlon = float(options['maxlon'])
        minlat = float(options['minlat'])
        maxlat = float(options['maxlat'])
        maxnum = eval(options['maxnum'])
        data_uri, metadata_uri = map(uris_from_args, [options['data_file'], options['metadata_file']])
        count = SVPDrifter.objects.get_or_create(metadata_uri[0], data_uri[0],
                                                 time_coverage_start=start,
                                                 time_coverage_end=stop,
                                                 minlat=minlat, maxlat=maxlat,
                                                 minlon=minlon, maxlon=maxlon,
                                                 maxnum=maxnum)
        print 'Added %s new drifter datasets' % count

