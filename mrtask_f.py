# How does revenue vary over time? Calculate the average trip revenue per month - analysing it by hour of the day (day vs night) and the day of the week (weekday vs weekend).

from mrjob.job import MRJob
from mrjob.step import MRStep  
from datetime import datetime

class AverageRevenueByTime(MRJob):  

    def steps(self):  
        return [
            MRStep(
                mapper=self.map_revenue_by_time,
                reducer=self.reduce_average_revenue
            )
        ]

    def parse_datetime(self, datetime_str):  
        formats = ['%d-%m-%Y %H:%M:%S', '%d-%m-%Y %H:%M', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S']
        for fmt in formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                pass
        raise ValueError('no valid date format found')

    def map_revenue_by_time(self, _, line):  
        if not line.startswith('VendorID'):
            try:  
                fields = line.strip().split(',')  
                revenue = float(fields[16])
                pickup_datetime = self.parse_datetime(fields[1])
                month = pickup_datetime.month
                hour = pickup_datetime.hour
                weekday = pickup_datetime.weekday()  
                yield (month, hour, weekday), revenue
            except (ValueError, IndexError) as e:
                self.increment_counter('mapper_errors', 'invalid_line')
                self.log.warning(f"Skipping invalid line: {line.strip()} - Error: {e}")
                pass

    def reduce_average_revenue(self, key, values): 
        total_revenue = 0
        num_trips = 0

        for revenue in values:
            total_revenue += revenue
            num_trips += 1

        average_revenue = total_revenue / num_trips if num_trips > 0 else 0  
        yield key, average_revenue


if __name__ == '__main__':
    AverageRevenueByTime.run()
