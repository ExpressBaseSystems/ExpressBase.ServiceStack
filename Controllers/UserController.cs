
using Microsoft.AspNetCore.Mvc;

using System;
using System.Collections.Generic;
using System.Linq;

using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Controllers
{
    public class UserController : Controller
    {
        //
        // GET: /User/
        public ActionResult Index()
        {
            return View();
        }
        public ActionResult masterhome()
        {
            return View();
        }
        [HttpGet]
        public ActionResult Login()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Login(Models.UserModel user)
        {
          
            if (ModelState.IsValid)
            {
                if (await user.IsValid(user.UserName, user.Password))
                {
                    //FormsAuthentication.SetAuthCookie(user.UserName, user.RememberMe);
                    return RedirectToAction("masterhome", "User");
                }
                else
                {
                    ModelState.AddModelError("", "Login data is incorrect!");
                }
            }

            return View("Login");
        }
        public ActionResult Register()
        {
            return View();
        }
       


    }

}
