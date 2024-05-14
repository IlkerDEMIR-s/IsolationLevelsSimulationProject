using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;


namespace Simulation.FormUI
{
    public partial class IsolationLevelsSimulationForm : Form
    {
        static string connectionString = "Server=SQLEXPRESS;Database=AdventureWorks2022;User Id=[ID];Password=[PASSWORD];Connect Timeout=240";

        //Connect Timeout=[ ]; Increase the timeout period to complete the operation and give SQL Server more time.

        public int numberOfRunTransactions = 10; //100  
        public int deadlockCount = 0;
        public int[][] deadlockCounts = new int[2][] { new int[4], new int[4] };

        public int typeAUsers; // 5
        public int typeBUsers; // 8

        public IsolationLevelsSimulationForm()
        {
            InitializeComponent();
        }


        private void IsolationLevelsSimulationForm_Load(object sender, EventArgs e)
        {
            alertLbl.ForeColor = Color.Red;
            alertDeadlockLbl.ForeColor = Color.Red;
            alertLbl.Text = "Please enter the number of users for Type A and Type B transactions.";
            logTbx.Text = "The log will be displayed here.\n";
        }

        private void runBtn_Click(object sender, EventArgs e)
        {

            try
            {
                typeAUsers = Convert.ToInt32(TypeAUsersTbx.Text);
                typeBUsers = Convert.ToInt32(TypeBUsersTbx.Text);

                alertLbl.Text = "Starting database simulation...";

                RunAsync(typeAUsers, typeBUsers);
            }
            catch (Exception ex)
            {
                alertLbl.Text = "Please enter valid numbers for the number of users!";
                //Stop simulation
                return;
            }

        }

        private async Task RunAsync(int typeAUsers, int typeBUsers)
        {
            alertLbl.Text = "Running database simulation...";

            await SimulateDatabaseTransactions(typeAUsers, typeBUsers);

        }

        public async Task SimulateDatabaseTransactions(int typeAUsers, int typeBUsers)
        {

            alertLbl.Text = "Simulating database transactions...";

            // Array of isolation levels
            string[] isolationLevels = { "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE" };

            // Variables to store processing times and deadlock counts for each isolation level
            double[] typeA_durations = new double[isolationLevels.Length];
            double[] typeB_durations = new double[isolationLevels.Length];

            // Perform the simulation for each isolation level
            for (int i = 0; i < isolationLevels.Length; i++)
            {
                string isolationLevel = isolationLevels[i];

                alertLbl.Text = $"Simulation for Isolation Level: {isolationLevel}";

                //Initialize transactions for Type A users
                Task[] typeATasks = new Task[typeAUsers];
                for (int j = 0; j < typeAUsers; j++)
                {
                    typeATasks[j] = Task.Run(async () =>
                    {
                        double duration = await SimulateTypeAUser(isolationLevel);
                        typeA_durations[i] += duration;
                    });
                }

                //Initialize Type B users' transactions
                Task[] typeBTasks = new Task[typeBUsers];
                for (int j = 0; j < typeBUsers; j++)
                {
                    typeBTasks[j] = Task.Run(async () =>
                    {
                        double duration = await SimulateTypeBUser(isolationLevel);
                        typeB_durations[i] += duration;
                    });
                }

                //Wait for all operations to complete
                await Task.WhenAll(typeATasks);
                await Task.WhenAll(typeBTasks);

                //Calculate average processing times
                typeA_durations[i] /= typeAUsers;
                typeB_durations[i] /= typeBUsers;

                // Create textbox names specific to the isolation level
                string typeAUsersTextBoxName = $"NumberofTypeAUsersTbx{i + 1}";
                string typeBUsersTextBoxName = $"NumberofTypeBUsersTbx{i + 1}";
                string averageDurationOfTypeATbxName = $"AverageDurationOfTypeATbx{i + 1}";
                string averageDurationOfTypeBTbxName = $"AverageDurationOfTypeBTbx{i + 1}";
                string deadlocksEncounteredByTypeATbxName = $"DeadlocksEncounteredByTypeATbx{i + 1}";
                string deadlocksEncounteredByTypeBTbxName = $"DeadlocksEncounteredByTypeBTbx{i + 1}";

                // Find textboxes specific to the isolation level
                TextBox typeAUsersTextBox = (TextBox)this.Controls.Find(typeAUsersTextBoxName, true).FirstOrDefault();
                TextBox typeBUsersTextBox = (TextBox)this.Controls.Find(typeBUsersTextBoxName, true).FirstOrDefault();
                TextBox averageDurationOfTypeATbx = (TextBox)this.Controls.Find(averageDurationOfTypeATbxName, true).FirstOrDefault();
                TextBox averageDurationOfTypeBTbx = (TextBox)this.Controls.Find(averageDurationOfTypeBTbxName, true).FirstOrDefault();
                TextBox deadlocksEncounteredByTypeATbx = (TextBox)this.Controls.Find(deadlocksEncounteredByTypeATbxName, true).FirstOrDefault();
                TextBox deadlocksEncounteredByTypeBTbx = (TextBox)this.Controls.Find(deadlocksEncounteredByTypeBTbxName, true).FirstOrDefault();

                // Fill textboxes specific to the isolation level
                if (typeAUsersTextBox != null)
                {
                    typeAUsersTextBox.Text = typeAUsers.ToString();
                }

                if (typeBUsersTextBox != null)
                {
                    typeBUsersTextBox.Text = typeBUsers.ToString();
                }

                if (averageDurationOfTypeATbx != null)
                {
                    averageDurationOfTypeATbx.Text = $"{typeA_durations[i]} ms".ToString();
                }

                if (averageDurationOfTypeBTbx != null)
                {
                    averageDurationOfTypeBTbx.Text = $"{typeB_durations[i]} ms".ToString();
                }

                if (deadlocksEncounteredByTypeATbx != null)
                {
                    deadlocksEncounteredByTypeATbx.Text = deadlockCounts[0][i].ToString();
                }

                if (deadlocksEncounteredByTypeBTbx != null)
                {
                    deadlocksEncounteredByTypeBTbx.Text = deadlockCounts[1][i].ToString();
                }
            }

            alertLbl.Text = "Database simulation completed.";
            alertDeadlockLbl.Text = "Number of Deadlocks Encountered: " + deadlockCount;

        }

        public async Task<double> SimulateTypeAUser(string isolationLevel)
        {
            int isolationLevelIndex = Array.IndexOf(new string[] { "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE" }, isolationLevel);

            DateTime beginTime = DateTime.Now;

            Random rand = new Random(); // Create a new Random object for each thread

            for (int i = 0; i < numberOfRunTransactions; i++)
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    SqlCommand command = connection.CreateCommand();
                    SqlTransaction transaction = null; // Initialize transaction

                    try
                    {
                        // Set isolation level
                        command.CommandText = $"SET TRANSACTION ISOLATION LEVEL {isolationLevel}";
                        await command.ExecuteNonQueryAsync();

                        //Start the process
                        transaction = connection.BeginTransaction();

                        //Assign the action to the command object
                        command.Transaction = transaction;

                        // Run random update queries
                        for (int j = 0; j < 5; j++)
                        {
                            if (rand.NextDouble() < 0.5)
                            {
                                // Generate random start and end dates
                                DateTime beginDate = new DateTime(2011 + rand.Next(5), 1, 1); // Random year between 2011 and 2015
                                DateTime endDate = new DateTime(beginDate.Year, 12, 31); // End date is end of the same year

                                command.CommandText = "UPDATE Sales.SalesOrderDetail " +
                                                      "SET UnitPrice = UnitPrice * 10.0 / 10.0 " +
                                                      "WHERE UnitPrice > 100 " +
                                                      "AND EXISTS (SELECT * FROM Sales.SalesOrderHeader " +
                                                      "WHERE Sales.SalesOrderHeader.SalesOrderID = Sales.SalesOrderDetail.SalesOrderID " +
                                                      $"AND Sales.SalesOrderHeader.OrderDate BETWEEN @BeginDate AND @EndDate " +
                                                      "AND Sales.SalesOrderHeader.OnlineOrderFlag = 1)";

                                // prevent SQL injection and handles dates more appropriately
                                command.Parameters.Clear();
                                command.Parameters.AddWithValue("@BeginDate", beginDate.ToString("yyyy-MM-dd"));
                                command.Parameters.AddWithValue("@EndDate", endDate.ToString("yyyy-MM-dd"));

                                await command.ExecuteNonQueryAsync();
                            }
                        }

                        // Complete the transaction
                        transaction.Commit();
                    }
                    catch (SqlException ex)
                    {
                        // Check if the exception is due to deadlock
                        if (ex.Number == 1205) // Deadlock error number
                        {
                            Interlocked.Increment(ref deadlockCount); // Increment deadlock count
                            Interlocked.Increment(ref deadlockCounts[0][isolationLevelIndex]); //Increase deadlock count
                            logTbx.AppendText($"Deadlock occurred: {ex.Message}\n");
                            logTbx.AppendText("IT WAS DEADLOCK!!!!!!!!!!\n");
                            alertLbl.Text = "Deadlock occurred!";


                            // Rollback the transaction in case of any error 
                            if (transaction != null)
                                transaction.Rollback();
                        }
                        else
                        {
                            // Handle error status
                            logTbx.AppendText($"An error occurred: {ex.Message}\n");
                            logTbx.AppendText("IT WAS AN ERROR!!!!!!!!!!\n");
                            alertLbl.Text = "An error occurred!";


                            // Rollback the transaction in case of any error 
                            if (transaction != null)
                                transaction.Rollback();
                        }

                    }
                    finally
                    {
                        // Close the connection
                        connection.Close();
                    }
                }
            }

            DateTime endTime = DateTime.Now;
            TimeSpan elapsedTime = endTime - beginTime;

            return elapsedTime.TotalMilliseconds;
        }

        public async Task<double> SimulateTypeBUser(string isolationLevel)
        {
            int isolationLevelIndex = Array.IndexOf(new string[] { "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE" }, isolationLevel);

            DateTime beginTime = DateTime.Now;

            Random rand = new Random(); // Create a new Random object for each thread

            for (int i = 0; i < numberOfRunTransactions; i++)
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    SqlCommand command = connection.CreateCommand();
                    SqlTransaction transaction = null; // Initialize transaction

                    try
                    {
                        // Set isolation level
                        command.CommandText = $"SET TRANSACTION ISOLATION LEVEL {isolationLevel}";
                        await command.ExecuteNonQueryAsync();

                        //Start the process
                        transaction = connection.BeginTransaction();

                        //Assign the action to the command object
                        command.Transaction = transaction;

                        // Run random update queries
                        for (int j = 0; j < 5; j++)
                        {
                            if (rand.NextDouble() < 0.5)
                            {
                                // Generate random start and end dates
                                DateTime beginDate = new DateTime(2011 + rand.Next(5), 1, 1); // Random year between 2011 and 2015
                                DateTime endDate = new DateTime(beginDate.Year, 12, 31); // End date is end of the same year

                                command.CommandText = "SELECT SUM(Sales.SalesOrderDetail.OrderQty) " +
                                                      "FROM Sales.SalesOrderDetail " +
                                                      "WHERE UnitPrice > 100 " +
                                                      "AND EXISTS (SELECT * FROM Sales.SalesOrderHeader " +
                                                      "WHERE Sales.SalesOrderHeader.SalesOrderID = Sales.SalesOrderDetail.SalesOrderID " +
                                                      $"AND Sales.SalesOrderHeader.OrderDate BETWEEN @BeginDate AND @EndDate " +
                                                      "AND Sales.SalesOrderHeader.OnlineOrderFlag = 1)";

                                // prevent SQL injection and handles dates more appropriately
                                command.Parameters.Clear();
                                command.Parameters.AddWithValue("@BeginDate", beginDate.ToString("yyyy-MM-dd"));
                                command.Parameters.AddWithValue("@EndDate", endDate.ToString("yyyy-MM-dd"));

                                await command.ExecuteScalarAsync();
                            }
                        }

                        // Complete the transaction
                        transaction.Commit();
                    }
                    catch (SqlException ex)
                    {
                        // Check if the exception is due to deadlock
                        if (ex.Number == 1205) // Deadlock error number
                        {
                            Interlocked.Increment(ref deadlockCount); // Increment deadlock count
                            Interlocked.Increment(ref deadlockCounts[1][isolationLevelIndex]); //Increase deadlock count
                            logTbx.AppendText($"Deadlock occurred: {ex.Message}\n");
                            logTbx.AppendText("IT WAS DEADLOCK!!!!!!!!!!\n");
                            alertLbl.Text = "Deadlock occurred!";

                            // Rollback the transaction in case of any error 
                            if (transaction != null)
                                transaction.Rollback();
                        }
                        else
                        {
                            // Handle error status
                            logTbx.AppendText($"An error occurred: {ex.Message}\n");
                            logTbx.AppendText("IT WAS AN ERROR!!!!!!!!!!\n");
                            alertLbl.Text = "An error occurred!";

                            // Rollback the transaction in case of any error 
                            if (transaction != null)
                                transaction.Rollback();
                        }

                    }
                    finally
                    {
                        // Close the connection
                        connection.Close();
                    }
                }
            }

            DateTime endTime = DateTime.Now;
            TimeSpan elapsedTime = endTime - beginTime;

            return elapsedTime.TotalMilliseconds;
        }


    }
}
