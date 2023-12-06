import os
print(f"\tLooking for project.sqlite database.")

if os.path.isfile('project.sqlite'):
    print(f"\t[SUCCESS] Found project.sqlite database.")
else:
    print(f"\t[ERROR]Could not find it.")