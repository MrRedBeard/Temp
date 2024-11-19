using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Mvc;
using WebApp.DataAccess;
using System.Threading.Tasks;
using System.Threading;

namespace WebApp.Controllers
{
    //Access control for non-authenticated users and/or kiosk mode
    [AllowAnonymous]
    public class CrudController : BaseController
    {
        CrudData data = new CrudData();

        [HttpPost]
        public async Task<JsonResult> SearchAsync(string className, Dictionary<string, float> fields, Dictionary<string, float> terms, bool active, bool sortAscending, int? countLimit, Dictionary<string, object> limitObj)
        {
            ClassLibrary.clsDataStructure clsDataStructure = new ClassLibrary.clsDataStructure();
            List<KeyValuePair<string, bool>> classKeyValuePair = clsDataStructure.getTableKeyValuePair(className);
            string primaryKey = classKeyValuePair.Find(kvp => (kvp.Value)).Key;
            active = active && classKeyValuePair.Any(kvp => (kvp.Key == "Active")); //active won't work if Active is not defined

            try
            {
                var tokenSource = new CancellationTokenSource();
                var token = tokenSource.Token;
                var model = await data.SearchAsync(className, fields, terms, active, sortAscending, countLimit, limitObj, token);
                return Json(model, JsonRequestBehavior.AllowGet);
            }
            catch (Exception e)
            {
                return Json(new { success = false, message = e.Message }, JsonRequestBehavior.AllowGet);
            }
        }

        [HttpPost]
        public JsonResult Create(string className, Dictionary<string, object> obj)
        {
            ClassLibrary.clsDataStructure clsDataStructure = new ClassLibrary.clsDataStructure();
            List<KeyValuePair<string, bool>> classKeyValuePair = clsDataStructure.getTableKeyValuePair(className);
            string primaryKey = classKeyValuePair.Find(kvp => (kvp.Value)).Key;

            int index = (int?)obj[primaryKey] ?? 0;
            if (index == 0)
            {
                try
                {
                    var objOut = data.Create(className, classKeyValuePair, obj);
                    return Json(objOut, JsonRequestBehavior.AllowGet);
                }
                catch (Exception e)
                {
                    Response.StatusCode = 500;
                    Response.StatusDescription = "Something happended while creating the record.";
                    return Json(new { success = false, message = Response.StatusDescription }, JsonRequestBehavior.AllowGet);
                }
            }
            else
            {
                Response.StatusCode = 500;
                Response.StatusDescription = $"{primaryKey} did not equal 0.";
                return Json(new { success = false, message = Response.StatusDescription }, JsonRequestBehavior.AllowGet);
            }
        }

        [HttpPost]
        public JsonResult Get(string className, Dictionary<string, object> obj)
        {
            ClassLibrary.clsDataStructure clsDataStructure = new ClassLibrary.clsDataStructure();
            List<KeyValuePair<string, bool>> classKeyValuePair = clsDataStructure.getTableKeyValuePair(className);
            string primaryKey = classKeyValuePair.Find(kvp => (kvp.Value)).Key;

            try
            {
                var objOut = data.Get(className, classKeyValuePair, obj);
                return Json(objOut, JsonRequestBehavior.AllowGet);
            }
            catch (Exception e)
            {
                Response.StatusCode = 500;
                return Json(new { success = false, message = e.Message }, JsonRequestBehavior.AllowGet);
            }
        }

        [HttpPost]
        public JsonResult Update(string className, Dictionary<string, object> obj)
        {
            ClassLibrary.clsDataStructure clsDataStructure = new ClassLibrary.clsDataStructure();
            List<KeyValuePair<string, bool>> classKeyValuePair = clsDataStructure.getTableKeyValuePair(className);
            string primaryKey = classKeyValuePair.Find(kvp => (kvp.Value)).Key;

            int index = (obj.ContainsKey(primaryKey)) ? (int)obj[primaryKey] : 0;
            if (index > 0)
            {
                try
                {
                    var objOut = data.Update(className, classKeyValuePair, obj);
                    return Json(objOut, JsonRequestBehavior.AllowGet);
                }
                catch (Exception e)
                {
                    Response.StatusCode = 500;
                    Response.StatusDescription = "Object failed to update";
                    return Json(new { success = false, message = Response.StatusDescription }, JsonRequestBehavior.AllowGet);
                }
            }
            else
            {
                Response.StatusCode = 500;
                Response.StatusDescription = "No valid object selected to update.";
                return Json(new { success = false, message = Response.StatusDescription }, JsonRequestBehavior.AllowGet);
            }
        }

    }
}