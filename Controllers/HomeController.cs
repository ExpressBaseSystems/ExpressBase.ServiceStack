//using System.Web;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;
//using Microsoft.AspNetCore.Mvc;
//using System;
//using Microsoft.AspNetCore.Http;
//using System.IO;
//using ExpressBase.Common;
//using ExpressBase.Data;
//using ServiceStack;
//using ExpressBase.ServiceStack.Services;


//// For more information on enabling MVC for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

//namespace ExpressBase.ServiceStack
//{
//    public class HomeController : Controller
//    {
//        // GET: /<controller>/
//        public IActionResult Index()
//        {
//            return View();
//        }
//        public IActionResult About()
//        {
//            return View();
//        }
//        public IActionResult Contact()
//        {
//            return View();
//        }
//        [HttpPost]
//        public async Task<IActionResult> Loginuser(ExpressBase.ServiceStack.UserModel user)
//        {

//            if (ModelState.IsValid)
//            {
//                if (await user.IsValid(user.UserName, user.Password))
//                {
//                    // UserModel.IsLoggedIn = 1;
//                   // TempData["name"] = "Test data";
//                    if (user.RememberMe)
//                    {
//                        CookieOptions options = new CookieOptions();
//                        options.Expires = DateTime.Now.AddDays(15);
//                        Response.Cookies.Append("UserName", user.UserName, options);

//                    }
                    
//                    return RedirectToAction("formmenu", "Sample");
                    
                    
                   
//                }
//                else
//                {
//                    ModelState.AddModelError("", "Login data is incorrect!");
//                }
//            }

//            return View("Loginuser");
//        }
//        [HttpGet]
//        public IActionResult Loginuser()
//        {
//            UserModel model = new UserModel
//            {
//                RememberMe = true,
//                UserName = Request.Cookies["UserName"],
//            };

//            return View(model);
//        }
//        public IActionResult logout(ExpressBase.ServiceStack.UserModel user)
//        {
//            //UserModel.IsLoggedIn = 0;

//            return RedirectToAction("Index", "Home");
//            //View();
//        }

//        [HttpPost]
//        public IActionResult Registerview(Registermodel user)
//        {
//            var req = this.HttpContext.Request.Form;

//            EbModel ebmodel = new EbModel();
//            foreach (var obj in req)
//            {
//                ebmodel.PrimaryValues.Add(obj.Key, obj.Value);
                
//            }

//            if (ModelState.IsValid)
//            {
//                //Registermodel model = new Registermodel
//                //{
//                //   Profileimg = Request.Form.Files["Imageupload"]
//                // };

//                bool bStatus = false;
//                if (user.IsEdited ==false)
//                        bStatus = Insert(user);
//                else
//                    bStatus = Update(user);

//                if (bStatus)
//                    return RedirectToAction("masterhome", "Sample");
//                else
//                    ModelState.AddModelError("", "Entered data is incorrect!");
//            }
//            else
//            {
//                var errors = ModelState.Values.SelectMany(v => v.Errors);
//            }

//            return View("Registerview");
//        }

//        private bool Insert(Registermodel udata)
//        {
//            //byte[] img = ConvertToBytes(udata.Profileimg);

//            Dictionary<int, object> dict = new Dictionary<int, object>();
//            dict.Add(2847, udata.Email);
//            dict.Add(2848, udata.Password);
//            dict.Add(2850, udata.FirstName);
//            dict.Add(2851, udata.LastName);
//            dict.Add(2852, udata.MiddleName);
//            dict.Add(2853, udata.dob.ToString());
//            dict.Add(2854, udata.PhNoPrimary);
//            dict.Add(2855, udata.PhNoSecondary);
//            dict.Add(2856, udata.Landline);
//            dict.Add(2857, udata.Extension);
//            dict.Add(2858, udata.Locale);
//            dict.Add(2859, udata.Alternateemail);
//            //dict.Add(15, img);

//            JsonServiceClient client = new JsonServiceClient("http://localhost:53125/");
//            return client.Post<bool>(new Services.Register { TableId = 157, Colvalues = dict});
//        }
//        public static byte[] ConvertToBytes(IFormFile image)
//        {
//            byte[] imageBytes = null;

//            Stream stream = image.OpenReadStream();
//            BinaryReader reader = new BinaryReader(stream);
//            imageBytes = reader.ReadBytes((int)image.Length);
//            return imageBytes;
//        }

//        private bool Update(Registermodel udata)
//        {
//            Dictionary<int, object> dict = new Dictionary<int, object>();
//            dict.Add(2850, udata.FirstName);
//            dict.Add(2851, udata.LastName);
//            dict.Add(2852, udata.MiddleName);
//            dict.Add(2847, udata.Email);
//            dict.Add(2848, udata.Password);
//            dict.Add(2853, udata.dob.ToString("yyyy-MM-dd"));
//            dict.Add(2854, udata.PhNoPrimary);
//            dict.Add(2855, udata.PhNoSecondary);
//            dict.Add(2856, udata.Landline);
//            dict.Add(2857, udata.Extension);
//            dict.Add(2858, udata.Locale);
//            dict.Add(2859, udata.Alternateemail);
//            //dict.Add(2846, udata.id);

         
//            JsonServiceClient client = new JsonServiceClient("http://localhost:53125/");
//            return client.Post<bool>(new Services.EditUser { TableId = 157, Colvalues = dict,colid = 2846 });
//        }

//        public ActionResult Displaydata()
//        {
//            var e = LoadTestConfiguration();
//            DatabaseFactory df = new DatabaseFactory(e);

//            List<Displaydata> list1 = new List<Displaydata>();
//            string sql = "SELECT id,firstname,lastname,middlename FROM eb_users ";
//            var dt = df.ObjectsDatabase.DoQuery(sql);

//            foreach (EbDataRow dr in dt.Rows)
//            {
//                Displaydata dspdata = new Displaydata();
//                dspdata.id = Convert.ToInt32(dr[0]);
//                dspdata.FirstName = dr[1].ToString();
//                dspdata.LastName = dr[2].ToString();
//                dspdata.MiddleName = dr[3].ToString();
//                list1.Add(dspdata);
//            }

//            return View(list1);
//        }

//        private void InitDb(string path)
//        {
//            EbConfiguration e = new EbConfiguration()
//            {
//                ClientID = "xyz0007",
//                ClientName = "XYZ Enterprises Ltd.",
//                LicenseKey = "00288-22558-25558",
//            };
//            e.DatabaseConfigurations.Add(EbDatabases.EB_OBJECTS, new EbDatabaseConfiguration(EbDatabases.EB_OBJECTS, DatabaseVendors.PGSQL, "AlArz2014", "139.59.43.88", 5432, "postgres", "Opera754$", 500));
//            e.DatabaseConfigurations.Add(EbDatabases.EB_DATA, new EbDatabaseConfiguration(EbDatabases.EB_DATA, DatabaseVendors.PGSQL, "AlArz2014", "139.59.43.88", 5432, "postgres", "Opera754$", 500));
//            e.DatabaseConfigurations.Add(EbDatabases.EB_ATTACHMENTS, new EbDatabaseConfiguration(EbDatabases.EB_ATTACHMENTS, DatabaseVendors.PGSQL, "AlArz2014", "139.59.43.88", 5432, "postgres", "Opera754$", 500));
//            e.DatabaseConfigurations.Add(EbDatabases.EB_LOGS, new EbDatabaseConfiguration(EbDatabases.EB_LOGS, DatabaseVendors.PGSQL, "AlArz2014", "139.59.43.88", 5432, "postgres", "Opera754$", 500));

//            byte[] bytea = EbSerializers.ProtoBuf_Serialize(e);
//            EbFile.Bytea_ToFile(bytea, path);
//        }

//        public static EbConfiguration ReadTestConfiguration(string path)
//        {
//            return EbSerializers.ProtoBuf_DeSerialize<EbConfiguration>(EbFile.Bytea_FromFile(path));
//        }

//        private EbConfiguration LoadTestConfiguration()
//        {
//            InitDb(@"G:\xyz1.conn");
//            return ReadTestConfiguration(@"G:\xyz1.conn");
//        }

//        [HttpGet]
//        public ActionResult Registerview(int Id)
//        {
//            if(Id > 0)
//            {
//                //var id = Convert.ToInt32(Context.Request.Query["id"]);
//                string html = string.Empty;
//                IServiceClient client = new JsonServiceClient("http://localhost:53125/").WithCache();
//                var fr = client.Get<ViewResponse>(new ViewUser { ColId = Id });
//                Registermodel regis = new Registermodel
//                {

//                    FirstName = fr.Viewvalues["firstname"].ToString(),
//                    LastName = fr.Viewvalues["lastname"].ToString(),
//                    MiddleName = fr.Viewvalues["middlename"].ToString(),
//                    Email = fr.Viewvalues["email"].ToString(),
//                    dob = Convert.ToDateTime(fr.Viewvalues["dob"]),
//                    Password = fr.Viewvalues["pwd"].ToString(),
//                    PhNoPrimary = fr.Viewvalues["phnoprimary"].ToString(),
//                    PhNoSecondary = fr.Viewvalues["phnosecondary"].ToString(),
//                    Landline = fr.Viewvalues["landline"].ToString(),
//                    Extension = fr.Viewvalues["extension"].ToString(),
//                    Locale = fr.Viewvalues["locale"].ToString(),
//                    Alternateemail = fr.Viewvalues["alternateemail"].ToString(),
//                    IsEdited=true


//                };
//                return View(regis);
//            }
//            else
//            {

//                Registermodel register1 = new Registermodel
//                {
//                    IsEdited = false
//                };
//                return View(register1);
//            }
           
//        }
//    }
//}
