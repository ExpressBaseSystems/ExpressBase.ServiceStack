using ExpressBase.ServiceStack;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class UserModel
    {
        [Required]
        [Display(Name = "User name")]
        public string UserName { get; set; }

        [Required]
        [DataType(DataType.Password)]
        [Display(Name = "Password")]
        public string Password { get; set; }

        [Display(Name = "Remember on this computer")]
        public bool RememberMe { get; set; }

       // public static int IsLoggedIn { get; set; }

        /// <summary>
        /// Checks if user with given password exists in the database
        /// </summary>
        /// <param name="_username">User name</param>
        /// <param name="_password">User password</param>
        /// <returns>True if user exist and password is correct</returns>
        public async Task<bool> IsValid(string _username, string _password)
        {
            Dictionary<int, object> dict = new Dictionary<int, object>();
            dict.Add(2847, _username);
            dict.Add(2848, _password);
            JsonServiceClient client = new JsonServiceClient("http://localhost:53125/");
            LoginResponse res = await client.PostAsync<LoginResponse>(new Login { UserName = _username, Password = _password });
            return (res.AuthenticatedUser != null);
        }
    }
}
