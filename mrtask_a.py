# Which pickup location generates the most revenue?

from mrjob.job import MRJob
from mrjob.step import MRStep

class TripStats(MRJob):  # Renamed class

    def steps(self):
        return [
            MRStep(mapper=self.calculate_trip_data, reducer=self.aggregate_trip_data), 
            MRStep(reducer=self.find_best_vendor) 
        ]

    def calculate_trip_data(self, _, line): 
        if not line.startswith('VendorID'):
            try:
                data = line.strip().split(',')
                vendor_id = data[0]
                total_amount = float(data[16])
                trip_distance = float(data[4])
                passenger_count = int(data[3])
                yield vendor_id, (total_amount, trip_distance, passenger_count)

            except (ValueError, IndexError) as e:
                self.increment_counter('mapper_errors', 'invalid_line')
                self.logger.warn(f"Skipping invalid line: {line.strip()} - Error: {e}")
                pass

    def aggregate_trip_data(self, key, values):  
        total_revenue = 0
        total_distance = 0
        total_passengers = 0
        num_trips = 0

        for revenue, distance, passengers in values:
            total_revenue += revenue
            total_distance += distance
            total_passengers += passengers
            num_trips += 1

        avg_distance = total_distance / num_trips if num_trips > 0 else 0
        avg_passengers = total_passengers / num_trips if num_trips > 0 else 0

        yield None, (total_revenue, key, num_trips, avg_distance, avg_passengers)

    def find_best_vendor(self, _, values): 
        max_revenue = 0
        best_vendor = None
        trip_count_for_best_vendor = 0
        avg_dist_for_best_vendor = 0
        avg_pass_for_best_vendor = 0

        for revenue, vendor, num_trips, avg_distance, avg_passengers in values:
            if revenue > max_revenue:
                max_revenue = revenue
                best_vendor = vendor
                trip_count_for_best_vendor = num_trips
                avg_dist_for_best_vendor = avg_distance
                avg_pass_for_best_vendor = avg_passengers

        yield best_vendor, (max_revenue, trip_count_for_best_vendor, avg_dist_for_best_vendor, avg_pass_for_best_vendor)


if __name__ == '__main__':
    TripStats.run() 