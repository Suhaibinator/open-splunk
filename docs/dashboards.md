# Dashboard visualizations

A dashboard panel stores its SPL, time range, index scope, and visualization
settings together. Run saves pending dashboard edits, then executes that saved
panel search. Visualization settings control how returned rows are displayed;
they do not change the search, add aggregation, or request different time buckets.

Choose **Visualization** to select Table, Line, Area, Column, Bar, Pie, Single
value, or Scatter. Existing panels with no visualization settings continue to
show a table. Editing their title, SPL, or other dashboard settings does not add
visualization settings implicitly.

## Fields and values

Use **X field** for the horizontal coordinate or category and **Y fields** for
measures. The order of the Y fields determines the order of the displayed
series. **Series field** splits Cartesian rows by a returned group value.
Selections use exact result field names; display names are labels only. Settings
can be saved before running the search, but the returned schema and values must
support the selected chart.

| Visualization | Required result shape |
| --- | --- |
| Table | Any typed result rows; columns retain their server order. |
| Line, Area | One X field and at least one numeric Y field; rows keep their returned order and null values leave gaps. |
| Column, Bar | One X field and at least one numeric Y field; every row remains a distinct category, including repeated labels. |
| Pie | One X label and exactly one nonnegative numeric Y field per row. No categories are combined into an Other slice. |
| Single value | Exactly one returned row and one selected scalar field. It does not total multiple rows. |
| Scatter | Numeric X and Y values. Numeric-looking strings are not numeric coordinates. |

Charts project the server's typed rows directly. Null and missing values are
never converted to zero; structured values are not flattened into chart
coordinates. A chart whose configuration or values are incompatible displays
an explanation. **Inspect rows** exposes its original rows so the query or
settings can be corrected.

Chart positions can be approximate when a number cannot be represented exactly
in the browser. Value labels and the value inspector retain the exact server
representation, including large integers and decimals. Focus an inspector
control and use the arrow keys to move between rows. Chart identity and exact
values remain available without relying on color.

## Presentation settings

**Visualization title**, **Show legend**, and **Show data labels** customize the
chart's presentation. **Stacking** selects separate, stacked, or 100% stacked
series where the visualization supports them. Stacking is a display operation;
it does not replace the original values shown in the inspector.

**Time bucket width (seconds)** declares expected spacing for time coordinates.
It controls visible gaps between returned points. It does not rebucket rows,
fill absent rows, change the SPL, or change the search time range. Set the bucket
size in the SPL itself when the query needs different aggregation; see
[Timechart](timechart.md).

## Result coverage and lifecycle

Tables show 20 rows per page. **Next results page** and **Previous results page**
use the same completed search job's cursors.

Charts collect at most 10,000 rows from one result snapshot. Coverage text states
whether the collected result is complete or limited. A limited chart represents
only its stated retained rows; it does not summarize rows beyond the limit.
Change the search when an explicit aggregation or smaller result is needed.

At most four panel pipelines run concurrently, including job polling and result
collection. A newer run, panel edit or removal, or dashboard change cancels stale
work so late responses cannot replace the current panel. A failed or expired
search displays an error and can be run again.
