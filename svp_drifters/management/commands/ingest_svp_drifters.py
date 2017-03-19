from django.utils import timezone
from django.core.management.base import BaseCommand

from geospaas.utils import uris_from_args
from svp_drifters.models import SVPDrifter

class Command(BaseCommand):
    args = '<metadata filename> <data filename> <yyyy-mm-dd> <yyyy-mm-dd> <# of drifters>'
    help = 'Add drifter metadata to archive'

    def handle(self, *args, **options):
        if not len(args)>=2:
            raise IOError('Please provide two filenames')

        uris = uris_from_args(args[0], args[1])
        num = None
        start = None
        end = None
        if len(args)>=3:
            start = timezone.datetime.strptime(args[2],
                    '%Y-%m-%d').replace(tzinfo=timezone.utc)
        if len(args)>=4:
            end = timezone.datetime.strptime(args[3],
                    '%Y-%m-%d').replace(tzinfo=timezone.utc)
        if len(args)>=5:
            num=int(args[4])

        count = SVPDrifter.objects.add_svp_drifters(uris[0], uris[1],
                time_coverage_start=start, time_coverage_end=end, maxnum=num)
        print 'Added %d new drifter datasets'%count

