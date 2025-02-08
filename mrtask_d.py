# What is the average trip time for different pickup locations?

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class AverageTripTime(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.map_trip_times,
                combiner=self.combine_trip_times,
                reducer=self.reduce_trip_times
            )
        ]

    def parse_datetime(self, datetime_str):
        formats = ['%d-%m-%Y %H:%M:%S', '%d-%m-%Y %H:%M', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S']
        for fmt in formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                pass
        raise ValueError('No valid date format found')

    def map_trip_times(self, _, line):
        if not line.startswith('VendorID'):
            try:
                fields = line.strip().split(',')
                pickup_location = fields[7]
                pickup_datetime = self.parse_datetime(fields[1])
                dropoff_datetime = self.parse_datetime(fields[2])
                trip_time = (dropoff_datetime - pickup_datetime).total_seconds() / 60.0
                yield pickup_location, (trip_time, 1)
            except (ValueError, IndexError) as e:
                self.increment_counter('mapper_errors', 'invalid_line')
                pass

    def combine_trip_times(self, pickup_location, trip_times):
        total_trip_time = 0
        total_count = 0
        for trip_time, count in trip_times:
            total_trip_time += trip_time
            total_count += count
        yield pickup_location, (total_trip_time, total_count)

    def reduce_trip_times(self, pickup_location, trip_times):
        total_trip_time = 0
        total_count = 0
        for trip_time, count in trip_times:
            total_trip_time += trip_time
            total_count += count
        average_trip_time = total_trip_time / total_count if total_count > 0 else 0
        yield pickup_location, average_trip_time


if __name__ == '__main__':
    AverageTripTime.run()
