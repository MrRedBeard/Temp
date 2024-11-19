using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using ClassLibrary;
using DataModel;
using Newtonsoft.Json;

namespace WebApp.DataAccess
{
    public class CrudData : BaseData
    {

        private string SearchQueryBuilder(string className, Dictionary<string, float> fields, Dictionary<string, float> terms, bool active, bool sortAscending)
        {
            List<string> queryWhere = new List<string>();
            List<string> scoreMetrics = new List<string>();
            foreach (string field in fields.Keys)
            {
                // Plus(term weight * average field weight) for ANY term match
                scoreMetrics.Add($@"CASE 
                    WHEN {String.Join(" OR ", terms.Keys.Select(term => $"LOWER({field}) LIKE '%{term}%'"))} THEN {terms.Values.Average() * fields[field]}
                    ELSE 0
                END");
                foreach (string term in terms.Keys)
                {
                    queryWhere.Add($"LOWER({field}) LIKE '%{term}%'");
                    // Plus(term weight * field weight) for EACH term match
                    scoreMetrics.Add($@"CASE 
                        WHEN LOWER({field}) LIKE '%{term}%' THEN {terms[term] * fields[field]}
                        ELSE 0
                    END");
                }
            }

            string query = $"SELECT *, {String.Join(" + ", scoreMetrics)} _searchScore FROM {className}";
            if (active || queryWhere.Any())
            {
                query += " WHERE ";
                if (active)
                {
                    query += "Active=1";
                    if (queryWhere.Any())
                    {
                        query += " AND ";
                    }
                }
                query += String.Join(" OR ", queryWhere);
            }
            query += $" ORDER BY _searchScore {(sortAscending ? "ASC" : "DESC")}";

            return query;

            ////statistics start here
            //double[] scores = (res as IEnumerable<Dictionary<string, object>>).Select(entry => (double)entry["_searchScore"]).ToArray();

            //double MIN, MAX, AVG, MEDIAN, MODE, STDEV;
            //if (!(scores == null || scores.Length == 0))
            //{
            //    MIN = scores.Min();
            //    MAX = scores.Max();
            //    AVG = scores.Average();
            //    MEDIAN = GetMedian(scores);
            //    MODE = scores.GroupBy(v => v)
            //                    .OrderByDescending(g => g.Count())
            //                    .First()
            //                    .Key;
            //    double sumOfSquaresOfDifferences = scores.Select(val => (val - AVG) * (val - AVG)).Sum();
            //    STDEV = Math.Sqrt(sumOfSquaresOfDifferences / scores.Length);
            //}
            ////statistics end here
        }

        private List<string> GetMultipleQueryConditions(List<KeyValuePair<string, bool>> classKeyValuePair, Dictionary<string, object> obj)
        {
            string[] nonPrimaryKeys = classKeyValuePair.Where(kvp => (!kvp.Value && !kvp.Key.ToLower().Contains("json"))).Select(kvp => kvp.Key).ToArray();
            List<string> queryWhere = new List<string>();
            foreach (string key in nonPrimaryKeys)
            {
                if (obj[key] != null)
                {
                    string theType = obj[key].GetType().Name;
                    switch (theType)
                    {
                        case "String":
                            queryWhere.Add($"{key} = '{obj[key]}'");
                            break;
                        case "Boolean":
                            queryWhere.Add($"{key} = {((bool)obj[key] ? "1" : "0")}");
                            break;
                        default:
                            queryWhere.Add($"{key} = {obj[key]}");
                            break;
                    }

                }
                var a = obj[key];
            }
            return queryWhere;
        }

        public async Task<Object> SearchAsync(string className, Dictionary<string, float> fields, Dictionary<string, float> terms, bool active, bool sortAscending, int? countLimit, Dictionary<string, object> limitObj, System.Threading.CancellationToken token)
        {

            List<SearchStopWords> stopWords = db.Database.SqlQuery<SearchStopWords>("SELECT * FROM SearchStopWords").ToList();
            List<string> notTheseWords = stopWords.Select(stopWord => stopWord.word).ToList();
            terms = terms.Where(i => !notTheseWords.Contains(i.Key)).ToDictionary(i => i.Key, i => i.Value);

            string query = SearchQueryBuilder(className, fields, terms, active, sortAscending);

            //The following further limits search results by object instance constraints, similar to "Get All"
            ClassLibrary.clsDataStructure clsDataStructure = new ClassLibrary.clsDataStructure();
            List<KeyValuePair<string, bool>> classKeyValuePair = clsDataStructure.getTableKeyValuePair(className);
            List<string> limitQueryWhere = GetMultipleQueryConditions(classKeyValuePair, limitObj);
            limitQueryWhere = limitQueryWhere.Where(condition => condition != "Active = 1").ToList();
            if (limitQueryWhere.Any())
            {
                string[] querySplit = query.Split(new[] { " ORDER BY _searchScore " }, StringSplitOptions.None);
                string queryBeforeOrder = querySplit[0];
                string queryAfterOrder = " ORDER BY _searchScore " + querySplit[1];
                if (queryBeforeOrder.Contains($" FROM {className} WHERE "))
                {
                    queryBeforeOrder = queryBeforeOrder.Replace($" FROM {className} WHERE", $" FROM {className} WHERE (");
                    queryBeforeOrder = queryBeforeOrder + ") AND (" + String.Join(" AND ", limitQueryWhere) + ")";
                } else
                {
                    queryBeforeOrder = queryBeforeOrder + " WHERE " + String.Join(" AND ", limitQueryWhere);
                }
                query = queryBeforeOrder + queryAfterOrder;
            }

            return await Task.Run(async () =>
            {
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    var data = await ObjectifyAsync(query).ConfigureAwait(false);
                    data = ObjectifyRemoveColumns(data, "");
                    if (token.IsCancellationRequested)
                    {
                        throw new AggregateException();
                    }
                    return data;
                }
            }, token);
        }

        private static double GetMedian(double[] sourceNumbers)
        {
            //make sure the list is sorted, but use a new array
            double[] sortedPNumbers = (double[])sourceNumbers.Clone();
            Array.Sort(sortedPNumbers);

            //get the median
            int size = sortedPNumbers.Length;
            int mid = size / 2;
            double median = (size % 2 != 0) ? (double)sortedPNumbers[mid] : ((double)sortedPNumbers[mid] + (double)sortedPNumbers[mid - 1]) / 2;
            return median;
        }

        private object ToObject(string className, Dictionary<string, object> obj)
        {
            Type theType = AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(assembly => assembly.GetTypes())
                .First(type => type.FullName == "TEAMModel." + className);

            object newObj = Activator.CreateInstance(theType);
            string json = JsonConvert.SerializeObject(obj);

            JsonConvert.PopulateObject(json, newObj);
            return newObj;
        }

        public object Create(string className, List<KeyValuePair<string, bool>> classKeyValuePair, Dictionary<string, object> obj)
        {
            string primaryKey = classKeyValuePair.Find(kvp => (kvp.Value)).Key;

            obj[primaryKey] = 0;
            if(CurrentToken != null)
            {
                obj["CreatedBy"] = CurrentToken.Payload.UserID;
                obj["LastUpdatedBy"] = CurrentToken.Payload.UserID;
            }
            obj["RecordCreatedDateTime"] = DateTime.Now;
            obj["RecordLastUpdateDateTime"] = DateTime.Now;
            obj["Active"] = true;

            object newObj = ToObject(className, obj);

            var dbSet = db.Set(newObj.GetType());
            dbSet.Add(newObj);
            db.SaveChanges();

            return newObj;
        }

        public object Get(string className, List<KeyValuePair<string, bool>> classKeyValuePair, Dictionary<string, object> obj)
        {
            try
            {
                string primaryKey = classKeyValuePair.Find(kvp => (kvp.Value)).Key;
                string query = $"SELECT * FROM {className} WHERE {className}.{primaryKey} = {obj[primaryKey]}";
                return ObjectifyRemoveColumns(Objectify(query), "");
            }
            catch (Exception e)
            {
                // No primary key matches, time for Plan B, which may return multiple results
                List<string> queryWhere = GetMultipleQueryConditions(classKeyValuePair, obj);
                string query = $"SELECT * FROM {className}";
                if (queryWhere.Any())
                {
                    query += $" WHERE {String.Join(" AND ", queryWhere)}";
                }
                return ObjectifyRemoveColumns(Objectify(query), "");
            }
        }

        public object Update(string className, List<KeyValuePair<string, bool>> classKeyValuePair, Dictionary<string, object> obj)
        {
            if (CurrentToken != null)
            {
                obj["LastUpdatedBy"] = CurrentToken.Payload.UserID;
            }
            obj["RecordLastUpdateDateTime"] = DateTime.Now;

            object newObj = ToObject(className, obj);

            var dbSet = db.Set(newObj.GetType());
            dbSet.Attach(newObj);
            db.Entry(newObj).State = System.Data.Entity.EntityState.Modified;
            db.SaveChanges();

            return newObj;
        }

    }
}