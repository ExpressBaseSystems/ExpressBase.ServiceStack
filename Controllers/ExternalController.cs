using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using ServiceStack;

namespace ExpressBase.ServiceStack.Controllers
{
    public class ExternalController : Controller
    {
        [HttpGet]
        public IActionResult LoginTenantUser(string clientid)
        {
            ViewBag.ClientId = clientid;
            return View();
        }

        [HttpPost]
        public IActionResult LoginTenantUser()
        {
            var req = this.HttpContext.Request.Form;
            AuthenticateResponse authResponse = null;

            try
            {
                var authClient = new JsonServiceClient("http://localhost:53125/");
                authResponse = authClient.Send(new Authenticate
                {
                    provider = MyJwtAuthProvider.Name,
                    UserName = req["uname"],
                    Password = req["pass"],
                    Meta = new Dictionary<string, string> { { "ClientId", req["clientid"] }, { "Login", "User" } },
                    UseTokenCookie = true
                });
            }catch(WebServiceException wse)
            {
                return View();
            }

            if (authResponse != null && authResponse.ResponseStatus != null 
                && authResponse.ResponseStatus.ErrorCode == "EbUnauthorized")
                return View();

            CookieOptions options = new CookieOptions();
            //options.Secure = true;

            Response.Cookies.Append("Token", authResponse.BearerToken, options);
            if (req.ContainsKey("remember"))
                Response.Cookies.Append("UserName", req["uname"], options);
           
            return RedirectToAction("formmenu", "Sample");
        }
    }
}
