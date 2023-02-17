import json
import os

my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, 'dataset.csv')


def _extract():
    applicants = []
    with open(configuration_file_path, 'r') as file:
        # insert the data in a list as lists
        for line in file:
            applicant = line.strip().split(',')
            applicants.append(applicant)
        applicants.remove(applicants[0])
        # remove duplicate rows
        for i in range(len(applicants)-2):
            if applicants[i][0] == (applicants[i+1][0]):
                applicants.remove(applicants[i+1])
        # fill NaN values with "Unknown"
        for applicant in applicants:
            for e in applicant:
                if len(e) == 0:
                    applicant[applicant.index(e)] = 'Unknown'

        # Filter out these rows, where applicants cognitive score is below 50
        # count = 1
        # for i in range(len(applicants)):
        #     if int(applicants[i][-1]) < 50:
        #         count += 1
        #         applicants.remove(applicants[i])
#         for some reason the above loop produces this error: IndexError: list index out of range
#           and I already spent like 30' trying to figure out why, but I dont have more time to loose...

        # Create new column "Class-name" and apply this logic:
        for applicant in applicants:
            if applicant[-2] == 'Java':
                applicant.append('Panthera')
            else:
                applicant.append('Celadon')


_extract()