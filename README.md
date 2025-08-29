How Options Work Together
The start_slideshow service's options combine to create the final photo list and its display order. The key is to understand the hierarchy of these options.

1. Recursive: The First Step
The recursive option is evaluated first.

recursive: true: The integration searches for photos in the main folder and all its subfolders. This is how the complete list of available photos is generated.

recursive: false: The integration only looks for photos directly inside the specified folder.

2. Shuffle: The Override
The shuffle option determines the primary sorting logic for the entire photo list generated in the first step.

shuffle: true: The entire list of photos is completely randomized. Any other sorting options, such as sort_folder_by_folder or resume, are ignored. The slideshow will always start from a new, random position and order.

shuffle: false: The photos are not randomized. This allows the other sorting and resume options to take effect.

3. Sort & Resume: The Details
If shuffle is false, the sort_folder_by_folder and resume options define the specific, non-random order of the slideshow.

sort_folder_by_folder	resume	Resulting Slideshow Behavior
true	true	The slideshow will resume from the last known photo. Photos are displayed in alphabetical order, sorted by folder. All photos in one folder are shown before moving to the next. The order is consistent across restarts.
true	false	The slideshow starts from the beginning of the photo list. Photos are displayed in alphabetical order, sorted by folder. The order is consistent across sessions.
false	true	The slideshow will resume from the last known photo. All photos from all folders are treated as one large list and sorted alphabetically. The order is consistent across sessions.
false	false	The slideshow starts from the beginning of the photo list. The entire library is sorted alphabetically as one single list, regardless of folders.
