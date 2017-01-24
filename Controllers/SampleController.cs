using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using ServiceStack;

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
            EbModel ebmodel = new EbModel();
            foreach(var obj in req)
            {
                if(obj.Key == "isUpdate")
                {
                    ebmodel.IsEdited = Convert.ToBoolean(obj.Value);
                }
                else if(obj.Key == "fId")
                {
                    ebmodel.FormId = Convert.ToInt32(obj.Value);
                }
                else
                {
                    ebmodel.PrimaryValues.Add(obj.Key, obj.Value);
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
                    ModelState.AddModelError("", "Entered data is incorrect!");
           

            return View();
        }
        private bool Insert(EbModel udata)
        {

            JsonServiceClient client = new JsonServiceClient("http://localhost:53125/");
            return client.Post<bool>(new Services.Register { TableId = 157, Colvalues = udata.PrimaryValues });
        }

        private bool Update(EbModel udata)
        {
           

            JsonServiceClient client = new JsonServiceClient("http://localhost:53125/");
            return client.Post<bool>(new Services.EditUser { TableId = 157, Colvalues = udata.PrimaryValues, colid = 2846 });
        }


    }
}
