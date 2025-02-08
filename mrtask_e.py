# Calculate the average tips to revenue ratio of the drivers for different pickup locations in sorted format.

from mrjob.job import MRJob
from mrjob.step import MRStep  

class AverageTipsToRevenueRatio(MRJob): 

    def steps(self):  
        return [
            MRStep(
                mapper=self.mapper_calculate_tips_revenue,
                combiner=self.combiner_aggregate_tips_revenue,
                reducer=self.reducer_calculate_ratio
            )
        ]

    def mapper_calculate_tips_revenue(self, _, line): 
        if not line.startswith('VendorID'):
            try:  
                fields = line.strip().split(',')  
                pickup_location = fields[7]
                total_revenue = float(fields[16])
                tips = float(fields[13])
                yield pickup_location, (tips, total_revenue)
            except (ValueError, IndexError) as e:
                self.increment_counter('mapper_errors', 'invalid_line')
                self.log.warning(f"Skipping invalid line: {line.strip()} - Error: {e}")
                pass

    def combiner_aggregate_tips_revenue(self, pickup_location, tips_revenues):  
        total_tips = 0
        total_revenue = 0
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        yield pickup_location, (total_tips, total_revenue)

    def reducer_calculate_ratio(self, pickup_location, tips_revenues): 
        total_tips = 0
        total_revenue = 0
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        average_tips_to_revenue_ratio = total_tips / total_revenue if total_revenue > 0 else 0  
        yield pickup_location, average_tips_to_revenue_ratio


if __name__ == '__main__':
    AverageTipsToRevenueRatio.run()
