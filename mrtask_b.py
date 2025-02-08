# Which pickup location generates the most revenue? 

from mrjob.job import MRJob
from mrjob.step import MRStep

class LocationWithHighestRevenue(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.map_location_revenue, reducer=self.reduce_location_revenue), 
            MRStep(reducer=self.find_highest_revenue_location)  
        ]

    def map_location_revenue(self, _, line):  
        # Skip header line
        if not line.startswith('VendorID'):
            fields = line.split(',')
            pickup_location = fields[7]
            revenue = float(fields[16])
            yield pickup_location, revenue

    def reduce_location_revenue(self, pickup_location, revenues):  
        yield None, (sum(revenues), pickup_location)

    def find_highest_revenue_location(self, _, max_revenues):  
        max_revenue, pickup_location = max(max_revenues)
        yield pickup_location, max_revenue


if __name__ == '__main__':
    LocationWithHighestRevenue.run()  