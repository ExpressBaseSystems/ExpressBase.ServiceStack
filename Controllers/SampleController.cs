using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using ServiceStack;
using ServiceStack.Redis;
using ExpressBase.Objects;

// For more information on enabling MVC for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace ExpressBase.ServiceStack
{
    public class SampleController : Controller
    {
        // GET: /<controller>/
        public IActionResult Index()
        {
            return View();
        }

        
        public IActionResult GetData()
        {

            

            return View();
        }

        public string sample()
        {
            
            return "{'name':'haii'}";       
        }

        public IActionResult Table()
        {
            return View();
        }
        public IActionResult Masterhome()
        {
            return View();
        }

        //[ValidateAntiForgeryToken]
        public IActionResult RForm(int id)
        {
            ViewBag.FormId = id;
            return View();
        }

        public IActionResult formmenu()
        {
            
            return View();
        }
        [HttpGet]
        public IActionResult f(int id)
        {
            ViewBag.FormId = id;
            return View();
        }
        
        [HttpPost]
        public IActionResult f()
        {
            var req = this.HttpContext.Request.Form;
            var id = Convert.ToInt32(req["fId"]);
            var redisClient = new RedisClient("139.59.39.130", 6379, "Opera754$");
            Objects.EbForm _form = redisClient.Get<Objects.EbForm>(string.Format("form{0}", id));
            EbModel ebmodel = new EbModel();
            
            ebmodel.TableId = _form.Table.Id;
            
            foreach (var obj in req)
            {
                var fobject = _form.GetControl(obj.Key);
                ebmodel.PrimaryValues.Add(obj.Key, obj.Value);
                if (obj.Key == "isUpdate")
                {
                    ebmodel.IsEdited = Convert.ToBoolean(obj.Value);
                }
                else if(obj.Key == "fId")
                {
                    ebmodel.FormId = Convert.ToInt32(obj.Value);
                }

              
            }
           
                bool bStatus = false;
               // if (Convert.ToBoolean(ebmodel.PrimaryValues["isUpdate"]) == false)
                    bStatus = Insert(ebmodel);
            //else
            //    bStatus = Update(ebmodel);

            if (bStatus)
                return RedirectToAction("masterhome", "Sample");
            else
                return RedirectToAction("Index", "Home");
           

            return View();
        }
        private bool Insert(EbModel udata)
        {

            JsonServiceClient client = new JsonServiceClient("http://localhost:53125/");
            return client.Post<bool>(new Services.Register { TableId = udata.TableId, Colvalues = udata.PrimaryValues });
        }

        private bool Update(EbModel udata)
        {
            JsonServiceClient client = new JsonServiceClient("http://localhost:53125/");
            return client.Post<bool>(new Services.EditUser { TableId = 157, Colvalues = udata.PrimaryValues, colid = 2846 });
        }


    }
}
