using ExpressBase.Data;
using ExpressBase.ServiceStack.Services;
using Microsoft.AspNetCore.Http;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class Displaydata
    {
        public int id { get; set; }
        
        public string FirstName { get; set; }

        public string LastName { get; set; }

        public string MiddleName { get; set; }

       

   


    }
}
