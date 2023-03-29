Scala Project that keep a listener on folder, to read all the files uploaded in a certain folder any time and adding discount depending on the qualification each row qualifies to and final price after discount then printing it into database.


**For reading files:
- opening a watcher that reads all the file copied to the folder in the path , and be always opened
- reading any amount of files copied 
- looping over those files, to process each file 1 by 1
- putting try - catch to be able incase reading any file had exception, to retry it 3 times, if failed, just skip it and read the next without closing
- applying the function of discount on each row 
- applying the function of final price which is calculated upon the final discount
- applying function to add a message into log file
- adding those 2 columns on data
- opening connection with MySql database
- writing the final data into table in the database
- rest the key to be able to read more files

**For discount function part:
- we had 6 qualifcation rules which is in the file attached , we made 2 functions, 1 to check if the data is qualified, and the other to calcualte the discount incase it's qualified
- made 12 function, 6 for checking, and 6 for calculating discounts.
- declared case class, to assign every check function and calculating discount functions as 1 element in the case class
- made a list of objects of case class, every object has both functions
- made a function to write the log file
- made function to calculate the discount, incase only 1 discount then print it, incase no discounts then print 0, incase more than 1 discount, get highest 2 and calculate average


**Team: 
1- Mohamed Ahmed Fathy
2- Sara Salah
3- Islam Younis 
