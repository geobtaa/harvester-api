These are notes for constructing methods

1. Determining Temporal Coverage and Date Range

We need: the Alternative Title, Date Issued, and Date Modified

If a year or range is in the title, this should be the Temporal Coverage.
If there is no year or range in the title, and Date Modified exists, the Temporal Coverage should be "Last Modified": date modified value.
Else, Temporal Coverage should be left blank.


If Temporal Coverage exists, use this to construct the Date Range.
If Temporal Coverage is blank, use the Date Issued to construct the Date Range.
Else, Date Range should be left blank.


2. Reformatting the Creator / Place name in the titles

We need: the Alternative Title, Spatial Coverage, Creator

3. Add a date to the end of the title

We need the formatted Title, Temporal Coverage

If a value for Temporal Coverage exists, add it to the end of the Title in {}
Else, don't add a date

======
