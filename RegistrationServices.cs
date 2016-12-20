using ExpressBase.Common;
using ExpressBase.Security;
using Microsoft.AspNetCore.Mvc.Routing;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    [DataContract]
    [Route("/register", "POST")]
    public class Register : IReturn<LoginResponse>
    {
        [DataMember(Order = 1)]
        public string Email { get; set; }
        [DataMember(Order = 2)]
        public string Password { get; set; }
        [DataMember(Order = 3)]
        public string FirstName { get; set; }
        [DataMember(Order = 4)]
        public string LastName { get; set; }
        [DataMember(Order = 5)]
        public string MiddleName { get; set; }
        [DataMember(Order = 6)]
        public DateTime DOB { get; set; }
        [DataMember(Order = 7)]
        public string PhNoPrimary { get; set; }
        [DataMember(Order = 8)]
        public string PhNoSecondary { get; set; }
        [DataMember(Order = 9)]
        public string Landline { get; set; }
        [DataMember(Order = 10)]
        public string Extension { get; set; }
        [DataMember(Order = 11)]
        public string Locale { get; set; }
        [DataMember(Order = 12)]
        public string Alternateemail { get; set; }
    }
    [DataContract]
    public class RegisterResponse
    {
        [DataMember(Order = 1)]
        public bool RegisteredUser { get; set; }
    }
    [ClientCanSwapTemplates]
    public class RegisterService : Service
    {
        public RegisterResponse Any(Register request)
        {
            bool userval= User. Create(request.Email,request.Password,request.FirstName,request.LastName,request.MiddleName,request.DOB,request.PhNoPrimary,request.PhNoSecondary,request.Landline,request.Extension,request.Locale,request.Alternateemail);
            return new RegisterResponse
            {
                RegisteredUser = userval
            };
        }
    }
}
