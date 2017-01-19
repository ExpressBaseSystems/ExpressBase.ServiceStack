using System.Web;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using System;
using Microsoft.AspNetCore.Http;
using System.IO;


// For more information on enabling MVC for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace ExpressBase.ServiceStack
{
    public class HomeController : Controller
    {
        // GET: /<controller>/
        public IActionResult Index()
        {
            return View();
        }
        public IActionResult About()
        {
            return View();
        }
        public IActionResult Contact()
        {
            return View();
        }
        [HttpPost]
        public async Task<IActionResult> Loginuser(ExpressBase.ServiceStack.UserModel user)
        {

            if (ModelState.IsValid)
            {
                if (await user.IsValid(user.UserName, user.Password))
                {
                    UserModel.IsLoggedIn = 1;
                    
                    if (user.RememberMe)
                    {
                        CookieOptions options = new CookieOptions();
                        options.Expires = DateTime.Now.AddDays(15);
                        Response.Cookies.Append("UserName", user.UserName, options);
                       
                    }
                    return RedirectToAction("formmenu", "Sample");
                }
                else
                {
                    ModelState.AddModelError("", "Login data is incorrect!");
                }
            }

            return View("Loginuser");
        }
        [HttpGet]
        public IActionResult Loginuser()
        {
            UserModel model = new UserModel
            {
                RememberMe = true,
                UserName = Request.Cookies["UserName"],
            };

            return View(model);
        }
        public IActionResult logout(ExpressBase.ServiceStack.UserModel user)
        {
            UserModel.IsLoggedIn = 0;
           
            return RedirectToAction("Index", "Home");
             //View();
        }
        [HttpGet]
        public ActionResult Registerview()
        {
            return View("Registerview");
        }

        [HttpPost]
        public async Task<IActionResult> Registerview(ExpressBase.ServiceStack.Registermodel user)
        {

            if (ModelState.IsValid)
            {
                var errors = ModelState.Values.SelectMany(v => v.Errors);
                Registermodel model = new Registermodel
                {
                   Profileimg = Request.Form.Files["Imageupload"]
                 };
              if(user.MiddleName==null)
                {
                    user.MiddleName = "asdfgh";
                }

                if (await user.UserRegister(user.Email, user.Password, user.FirstName, user.LastName, user.MiddleName, user.dob, user.PhNoPrimary, user.PhNoSecondary, user.Landline, user.Extension, user.Locale, user.Alternateemail,user.Profileimg))
                {
                    //FormsAuthentication.SetAuthCookie(user.UserName, user.RememberMe);
                    return RedirectToAction("masterhome", "Sample");
                }
                else
                {
                    ModelState.AddModelError("", "Entered data is incorrect!");
                }
            }

            return View("Registerview");
        }
       
    }
}
