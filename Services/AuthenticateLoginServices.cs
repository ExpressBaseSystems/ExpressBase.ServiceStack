using ExpressBase.Common;
using ExpressBase.Security;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    [DataContract]
    [Route("/login", "POST")]
    public class Login : IReturn<LoginResponse>
    {
        [DataMember(Order = 1)]
        public string UserName { get; set; }

        [DataMember(Order = 2)]
        public string Password { get; set; }
    }

    [DataContract]
    public class LoginResponse
    {
        [DataMember(Order = 1)]
        public User AuthenticatedUser { get; set; }
    }

    [ClientCanSwapTemplates]
    public class LoginService : Service
    {
        public LoginResponse Any(Login request)
        {
            User u = User.GetDetails(request.UserName, request.Password);
            return new LoginResponse
            {
                AuthenticatedUser = u
            };
        }
    }
}
