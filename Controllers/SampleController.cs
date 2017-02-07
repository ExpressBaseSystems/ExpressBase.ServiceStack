using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using ServiceStack;
using ServiceStack.Redis;
using ExpressBase.Objects;
using ExpressBase.ServiceStack.Services;
using Microsoft.AspNetCore.Http;
using ExpressBase.Data;

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
        //[HttpGet]
        //public IActionResult f(int id)
        //{
        //    ViewBag.FormId = id;
        //    return View();
        //}


        [HttpGet]
        public IActionResult f(int fid, int id)
        {
            var redisClient = new RedisClient("139.59.39.130", 6379, "Opera754$");
            //Objects.EbForm _form = redisClient.Get<Objects.EbForm>(string.Format("form{0}", fid));
            Objects.EbForm _form = null;
            IServiceClient client = new JsonServiceClient("http://localhost:53125/").WithCache();
            var fr = client.Get<EbObjectResponse>(new EbObjectRequest { Id = fid });
            if (id > 0)
            {
                if (fr.Data.Count > 0)
                {
                    _form = Common.EbSerializers.ProtoBuf_DeSerialize<EbForm>(fr.Data[0].Bytea);
                    _form.Init4Redis();
                    _form.IsUpdate = true;
                    redisClient.Set<EbForm>(string.Format("form{0}", fid), _form);
                }
                string html = string.Empty;
                var vr = client.Get<ViewResponse>(new View { TableId = _form.Table.Id, ColId = id, FId = fid });
                redisClient.Set<EbForm>("cacheform", vr.ebform);
                ViewBag.EbForm = vr.ebform;
                ViewBag.FormId = fid;
                ViewBag.DataId = id;
                return View();
            }
            else
            {
                if (fr.Data.Count > 0)
                {
                    _form = Common.EbSerializers.ProtoBuf_DeSerialize<EbForm>(fr.Data[0].Bytea);
                    _form.Init4Redis();
                    _form.IsUpdate = false;
                    redisClient.Set<EbForm>(string.Format("form{0}", fid), _form);
                }
                ViewBag.EbForm = _form;
                ViewBag.FormId = fid;
                ViewBag.DataId = id;
                return View();
            }
        }

        [HttpPost]
        public IActionResult f()
        {

            var req = this.HttpContext.Request.Form;
            var fid = Convert.ToInt32(req["fId"]);
            var redisClient = new RedisClient("139.59.39.130", 6379, "Opera754$");
            Objects.EbForm _form = redisClient.Get<Objects.EbForm>(string.Format("form{0}", fid));
            bool b = _form.IsUpdate;
            ViewBag.EbForm = _form;
            ViewBag.FormId = fid;
            ViewBag.formcollection = req as FormCollection;
            //bool bStatus = Insert(req as FormCollection);

            //if (bStatus)
            //    return RedirectToAction("masterhome", "Sample");
            //else
            //    return RedirectToAction("Index", "Home");

            return View();
        }
        //private bool Insert(IFormCollection udata)
        //{
        //    JsonServiceClient client = new JsonServiceClient("http://localhost:53125/");
        //    return client.Post<bool>(new Services.FormPersistRequest { TableId = Convert.ToInt32(udata["TableId"]), Colvalues = udata.ToDictionary(dict => dict.Key, dict => (object)dict.Value) });
        //}

    }
}
