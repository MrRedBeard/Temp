using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using ClassLibrary;
using DataModel;
using System.Collections.Concurrent;
using Newtonsoft.Json;

namespace WebApp.DataAccess
{
    public class BaseData
    {
        public ClassLibrary.clsWebToken CurrentToken = new clsWebToken();

        public DataEntities db { get; set; }

        /// <summary>
        /// Default constructor that instantiates the Entity Modle
        /// that all other DataAccess files will access
        /// </summary>
        public BaseData()
        {
            db = new DataEntities();
        }

        /// <summary>
        /// Objectify - create objects from data where class does not exist, data is dynamic, columns change
        /// </summary>
        /// <returns></returns>
        #region Objectify Dynamic SQL Object for SQL Dynamic Data
        public object Objectify(string query)
        {
            var model = new object();
            var connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["Entities"].ConnectionString;
            var provider = new System.Data.EntityClient.EntityConnectionStringBuilder(connectionString).ProviderConnectionString;

            using (SqlConnection connection = new SqlConnection(provider))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    SqlDataReader reader = command.ExecuteReader();

                    model = ObjectifyDataSet(reader);
                }
            }

            return model;
        }
        #region Async Objectify
        public async Task<object> ObjectifyAsync(string query)
        {
            var model = new object();
            var connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["Entities"].ConnectionString;
            var provider = new System.Data.EntityClient.EntityConnectionStringBuilder(connectionString).ProviderConnectionString;

            // Create a SqlConnectionStringBuilder instance,
            // and ensure that it is set up for asynchronous processing.
            SqlConnectionStringBuilder asyncConnectionString =
                new SqlConnectionStringBuilder(provider);
            // Asynchronous method calls won't work unless you
            // have added this option, or have added
            // the clause "Asynchronous Processing=true"
            // to the connection string.
            asyncConnectionString.AsynchronousProcessing = true;
            // Build the SqlConnection connection string.
            using (var conn = new SqlConnection(provider))
            {
                using (var cmd = new SqlCommand())
                {

                    cmd.Connection = conn;
                    if (query != null)
                    {
                        cmd.CommandText = query;
                        cmd.CommandType = CommandType.Text;
                        await conn.OpenAsync().ConfigureAwait(false);
                        SqlDataReader reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                        //below method, and child methods will need to be made async too i think
                        model = await ObjectifyDataSetAsync(reader).ConfigureAwait(false);

                    } else
                    {
                        throw new Exception("Query was null!");
                    }

                }
            }

            return model;
        }
        public async Task<IEnumerable<ConcurrentDictionary<string, object>>> ObjectifyDataSetAsync(SqlDataReader reader)
        {
            var results = new List<Task<ConcurrentDictionary<string, object>>>();
            var cols = new List<string>();
            Parallel.For(0, reader.FieldCount, (i) =>
            {
                cols.Add(reader.GetName(i));
            });
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                results.Add(ObjectifyRowParallel(cols, reader));
            }
            return await Task.WhenAll(results).ConfigureAwait(false);
        }
        private async Task<ConcurrentDictionary<string, object>> ObjectifyRowParallel(IEnumerable<string> cols, SqlDataReader reader)
        {
            var result = new ConcurrentDictionary<string, object>();

            Parallel.ForEach(cols, col =>
            {
                if(col == null)
                {
                    return;
                }
                col = col.ToString();
                var item = reader[col];
                result.TryAdd(col, item);
            });
            return await Task.FromResult(result).ConfigureAwait(false);
        }
        #endregion

        public object ObjectifyRemoveColumns(IEnumerable<object> obj, string columnsToRemove)
        {
            var results = ObjectifyRemoveColumnsFunc(obj, columnsToRemove);

            return results;
        }

        public object ObjectifyRemoveColumns(object obj, string columnsToRemove)
        {
            List<object> objs = new List<object>();
            objs.Add(obj);
            var results = ObjectifyRemoveColumnsFunc(objs, columnsToRemove);

            return results;
        }

        public object ObjectifyRemoveColumnsFunc(IEnumerable<object> obj, string columnsToRemove)
        {
            List<string> ColumnsToRemove = new List<string>();
            ColumnsToRemove.AddRange("SocialSecurityNumber,SSN".ToLower().Split(','));
            ColumnsToRemove.AddRange(columnsToRemove.ToLower().Split(','));

            var json = JsonConvert.SerializeObject(obj.ElementAt(0));
            var arrayObj = JsonConvert.DeserializeObject<IEnumerable<Dictionary<string,object>>>(json).ToArray();

            for (int i = 0; i < arrayObj.Count(); i++)
            {
                var item = arrayObj[i];
                List<string> badKeys = new List<string>();
                foreach (KeyValuePair<string, object> entry in item)
                {
                    if (ColumnsToRemove.Contains(entry.Key.ToLower()))
                    {
                        badKeys.Add(entry.Key);
                    }
                }
                foreach (string badKey in badKeys)
                {
                    item.Remove(badKey);
                }
            }

            if (arrayObj.Count() > 1)
            {
                return arrayObj.ToList();
            }
            else
            {
                return arrayObj.FirstOrDefault();
            }
        }

        public IEnumerable<Dictionary<string, object>> ObjectifyDataSet(SqlDataReader reader)
        {
            var results = new List<Dictionary<string, object>>();
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                cols.Add(reader.GetName(i));
            }

            while (reader.Read())
            {
                results.Add(ObjectifyRow(cols, reader));
            }

            return results.ToArray();
        }

        private Dictionary<string, object> ObjectifyRow(IEnumerable<string> cols, SqlDataReader reader)
        {
            var result = new Dictionary<string, object>();
            foreach (var col in cols)
            {
                result.Add(col, reader[col]);
            }
            return result;
        }
        #endregion

        #region Objectify OleDb Data
        /// <summary>
        /// 
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="query">from [{0}$A6:AN50000] WHERE F2 <> '' ", "Sheet1", "A6:AN50000"</param>
        /// <returns></returns>
        public object ObjectifyOleDb(string filePath, string query)
        {
            /*
                ToDo://
                Add Arguments:
                    Column Names A0 - AN50000, First Row as Column Names, Array of Column Names to be used
                    Start Row
                    Start Column
                    End Row
                    End Column
                    Detect Data Types from excel/data source
                    Include OriginalFileName in data set
            */

            object model = new object();
            string connectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source='" + filePath + "';Extended Properties=\"Excel 8.0;HDR=NO;IMEX=1\";";
            query = String.Format("SELECT *, '" + filePath + "' as OriginalFileName from [{0}$A0:AN50000] WHERE F2 <> '' ", "Sheet1", "A0:AN50000");

            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                connection.Open();
                using (OleDbCommand command = new OleDbCommand(query, connection))
                {
                    OleDbDataReader reader = command.ExecuteReader();

                    model = ObjectifyOleDbDataSet(reader, query, connection).ToArray();
                }
            }

            return model;
        }
        public IEnumerable<Dictionary<string, object>> ObjectifyOleDbDataSet(OleDbDataReader reader, string query, OleDbConnection connection)
        {
            var results = new List<Dictionary<string, object>>();
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (i == 0)
                {
                    //File Name
                    cols.Add(reader.GetName(i));
                }
                else
                {
                    //F1 - FN Excel Header
                    break;
                }

            }

            //Get First Row Column Names
            using (OleDbDataAdapter da = new OleDbDataAdapter(query, connection))
            {
                DataSet dataSet = new DataSet();
                da.Fill(dataSet);

                for (int i = 1; i < dataSet.Tables[0].Columns.Count; i++)
                {
                    string col = dataSet.Tables[0].Rows[0][i].ToString();

                    //Remove Special Characters and spaces - class will need to mirror these modified names 
                    col = Regex.Replace(col, "[^a-zA-Z0-9]+", "", RegexOptions.Compiled);

                    cols.Add(col);
                }
            }


            while (reader.Read())
            {
                results.Add(ObjectifyOleDbRow(cols, reader));
            }

            return results;
        }
        private Dictionary<string, object> ObjectifyOleDbRow(IEnumerable<string> cols, OleDbDataReader reader)
        {
            var result = new Dictionary<string, object>();

            var columns = cols.ToArray();

            for (int i = 0; i < columns.Length; i++)
            {
                result.Add(columns[i], reader[i]);
            }

            return result;
        }
        #endregion
    }
}
