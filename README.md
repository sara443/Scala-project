Scala Project
This project is designed to keep a listener on a specified folder, read all files uploaded to that folder, apply discounts to each row based on specific qualifications, calculate the final price after discount, and store the resulting data in a MySQL database.

File Processing
The project uses a watcher to read all files copied to the specified folder path. The watcher is always open and can read any amount of files. Each file is processed individually with the following steps:

A try-catch block is used to read each file. If a file reading exception occurs, the program will retry reading the file three times. If it still fails, the program will skip the file and move to the next one without closing the watcher.
The discount function is applied to each row of the file.
The final price is calculated for each row based on the final discount.
A log file message is added to each row of data.
The resulting data is written to a table in the MySQL database.
The watcher is reset to read more files.
Discount Function
The project includes six qualification rules that determine whether a discount should be applied or not. To evaluate these rules, the following functions were created:

Six functions to check whether the data qualifies for each rule.
Six functions to calculate the discount based on each rule.
A case class is used to assign each check function and discount calculation function as one element.
A list of case class objects is created, with each object containing both functions.
A function is created to write a log file.
A function is created to calculate the discount. If there is only one discount, it is printed. If there are no discounts, 0 is printed. If there are more than one discounts, the highest two are used to calculate the average.
Team
This project was completed by the following team members:
Mohamed Ahmed Fathy
Sara Salah
Islam Younis
We hope that our project will provide an example of how to use functional programming in Scala to handle complex data processing tasks. Please feel free to contact us if you have any questions or feedback.
