# Map Reduce Implemantion of DIRT algorithm

================================================================================ 
Created by: Dor Shtarker & Vladimir Shargorodsky

================================================================================

This program is implemantation of DIRT algorithm, which calculates the similarity between paths and the mutual information.
The algorithm will calculate the similarity in 2 stages:
1. Mi calculation (Mutual information).
2. Similatiry calculation.

================================================================================ 
Instructions:

1. Create S3 bucket.
2. Compile all the projects with the command mvn compile.
3. Add your data in userInfo file in DIRTRunner folder.
4. Upload the jars from Mi and Sim to your bucket.
5. Run the program with DIRTRunner, then choose the option You want to run. (Maybe you need to download the biarcs files and change lines 62, 75, 89 and 102 with your bucket folder with 10/100 biarcs files, since the current folders may be deleted)
6. Wait until It finish. Your computer should play sound when the program finished.
7. Collect the data from your S3 bucket.

================================================================================
