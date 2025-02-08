# What are the different payment types used by customers and their count? The final results should be in a sorted format.

from mrjob.job import MRJob
from mrjob.step import MRStep

class PaymentTypeCounts(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.count_payment_types, combiner=self.combine_payment_type_counts, reducer=self.reduce_payment_type_counts),
            MRStep(reducer=self.sort_results)
        ]

    def count_payment_types(self, _, line):
        if not line.startswith('VendorID'):
            fields = line.strip().split(',')
            if len(fields) > 9:  # Check for valid data length
                payment_type = fields[9]
                yield payment_type, 1

    def combine_payment_type_counts(self, payment_type, counts):
        yield payment_type, sum(counts)

    def reduce_payment_type_counts(self, payment_type, counts):
        total_count = sum(counts)
        # Prepare data for sorting: count comes first for easy sorting
        yield None, (total_count, payment_type)

    def sort_results(self, _, count_payment_pairs):
        # Sort by count in descending order
        sorted_results = sorted(count_payment_pairs, reverse=True)
        for count, payment_type in sorted_results:
            yield payment_type, count

if __name__ == '__main__':
    PaymentTypeCounts.run()
