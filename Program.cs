using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace dailysql
{
    class Program
    {
        static bool isServer = false; //flag for setting the output. if server is true it will not show a cmd output

        static void Main(string[] args)
        {
            //load settings from file
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();

            isServer = configuration["isServer"] == "true";

            var locBackDirPath = configuration.GetConnectionString("localPath");
            var prodBackDirPath = configuration.GetConnectionString("serverPath");
            var locMdfDirPath = configuration.GetConnectionString("mdfPath");
            var version = configuration["version"];
            var doRemove = configuration["doRemove"];

            write(version);

            write("start");

            var locBackDir = new DirectoryInfo(locBackDirPath);

            try
            {
                var lastBackFile = new DirectoryInfo(prodBackDirPath)
                    .GetFiles()
                    .OrderByDescending(o => o.LastWriteTime)
                    .FirstOrDefault();

                var prodFileLength = lastBackFile.Length;

                var locBackFilePath = locBackDirPath + @"\" + lastBackFile.Name;

                if (doRemove == "true") 
                {
                    doRemoveFiles(locBackDir);
                }

                if (lastBackFile.Exists)
                {
                    copyFileAsync(lastBackFile.FullName, locBackFilePath);
                    displayCopyProgress(prodFileLength, locBackDir);
                }

                if (File.Exists(locBackFilePath))
                {                    
                    write("drop sql db");
                    SqlConnectionStringBuilder sqlConnectionBuilder = new SqlConnectionStringBuilder();
                    sqlConnectionBuilder.DataSource = "localhost";
                    sqlConnectionBuilder.UserID = configuration["dbuser"];
                    sqlConnectionBuilder.Password = configuration["dbpass"];
                    sqlConnectionBuilder.InitialCatalog = "master";
                    
                    var databaseName = configuration["dbname"];

                    var deletecommand = new StringBuilder();
                    var restorecommand = new StringBuilder();

                    deletecommand.Append(@"EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'" + databaseName + "';");
                    deletecommand.Append(@"USE [master];");
                    deletecommand.Append(@"DROP DATABASE [" + databaseName + "];");

                    restorecommand.Append(@"USE [master];");
                    restorecommand.Append(@"RESTORE DATABASE [" + databaseName + @"] FROM  DISK = N'" + locBackFilePath + @"' WITH  FILE = 1,  MOVE N'" + databaseName + @"_Daten' TO N'C:\Program Files\Microsoft SQL Server\MSSQL14.MSSQLSERVER\MSSQL\DATA\" + databaseName + @".MDF',  MOVE N'" + databaseName + @"_Protokoll' TO N'C:\\Program Files\\Microsoft SQL Server\\MSSQL14.MSSQLSERVER\\MSSQL\\DATA\\" + databaseName + @".LDF',  NOUNLOAD,  STATS = 5;");

                    using (SqlConnection connection = new SqlConnection(sqlConnectionBuilder.ConnectionString))
                    {
                        connection.Open();

                        using (SqlCommand sqlCmd = new SqlCommand(deletecommand.ToString(), connection))
                        {
                            write("drop db");
                            using (SqlDataReader sqlReader = sqlCmd.ExecuteReader())
                            {
                                while (sqlReader.Read())
                                {
                                    write(sqlReader.GetString(0));
                                }
                            }
                        }

                        var mdfDir = new DirectoryInfo(locMdfDirPath);

                        mdfDir.EnumerateFiles()
                            .Where(x => x.Name.Contains(databaseName + ".MDF"))
                            .ToList()
                            .ForEach(x => x.Delete());

                        using (SqlCommand sqlCmd = new SqlCommand(restorecommand.ToString(), connection))
                        {
                            write("restore db");
                            using (SqlDataReader sqlReader = sqlCmd.ExecuteReader())
                            {
                                while (sqlReader.Read())
                                {
                                    write(sqlReader.GetString(0));
                                }
                            }
                        }
                    }
                }
                else
                {
                    write("cant find file. copy failed");
                }
            }
            catch (SqlException e)
            {
                write(e.InnerException?.Message ?? e.Message);
            }

            write("fin");
        }

        private static void displayCopyProgress(long l, DirectoryInfo d)
        {
            var sf = ((l / 1024) / 1024);

            var lf = 0L;
            var slf = 0L;
            var sc = 0D;

            while (lf != l)
            {
                System.Threading.Thread.Sleep(1000);

                lf = d.GetFiles().FirstOrDefault().Length;
                slf = ((lf / 1024) / 1024);                        
                sc = Math.Round(((double)slf / (double)sf) * 100, 2);

                write(sc + " %  | " + slf + " MB von " + sf + " MB");
            }        

            write("file copy fin");
        }

        private static void doRemoveFiles(DirectoryInfo d)
        {
            write("Remove Files from: " + d.FullName);

            try
            {
                d.EnumerateFiles().ToList().ForEach(x => x.Delete());    
            }
            catch (System.Exception e)
            {
                write("cant remove files from local directory: " + e.Message);
                throw;
            }
        }

        private static void write(string msg)
        {
            if (!isServer)
            {
                System.Console.WriteLine(msg);
            }
        }

        private static async Task copyFileAsync(string sourceFile, string destinationFile)
        {
            write("file copy start");

            using (var sourceStream = new FileStream(
                sourceFile,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                4096,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                using (var destinationStream = new FileStream(
                    destinationFile,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    4096,
                    FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    await sourceStream.CopyToAsync(destinationStream);
                }
            }
        }
    }
}
