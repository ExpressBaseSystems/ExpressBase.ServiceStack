using ServiceStack;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.ServiceStack;
using ExpressBase.ServiceStack.Services;
using Microsoft.AspNetCore.Http;
using System.IO;

namespace ExpressBase.ServiceStack
{
    public class Registermodel
    {
        [Required]
        [Display(Name = "Email")]
        public string Email { get; set; }
        [Required]

        [DataType(DataType.Password)]
        [Display(Name = "Password")]
        public string Password { get; set; }
        [Required]
        [Display(Name = "First name")]
        public string FirstName { get; set; }
        [Required]
        [Display(Name = "Last name")]
        public string LastName { get; set; }
        // [Required]
        [Display(Name = "Middle name")]
        public string MiddleName { get; set; }
        [Required]
        [DataType(DataType.Date)]
        [DisplayFormat(DataFormatString = "{0:yyyy-MM-dd}", ApplyFormatInEditMode = true)]
        [Display(Name = "dob")]
        public DateTime dob { get; set; }
        [Required]
        [Display(Name = "Upload Image")]
        public IFormFile Profileimg { get; set; }
        [Required]
        [Display(Name = "PhNoPrimary")]
        public string PhNoPrimary { get; set; }
        [Required]
        [Display(Name = "PhNoSecondary")]
        public string PhNoSecondary { get; set; }
        [Required]
        [Display(Name = "Landline")]
        public string Landline { get; set; }
        [Required]
        [Display(Name = "Extension")]
        public string Extension { get; set; }
        [Required]
        [Display(Name = "Locale")]
        public string Locale { get; set; }
        [Required]
        [Display(Name = "Alternateemail")]
        public string Alternateemail { get; set; }

        public async Task<bool> UserRegister(string uname, string password, string fname, string lname, string mname, DateTime DOB, string pphno, string sphno, string land, string extension, string locale, string aemail,IFormFile imgprofile)
        {
            byte[] img = ConvertToBytes(imgprofile);
            JsonServiceClient client = new JsonServiceClient("http://localhost:53125/");
            RegisterationResponse res = await client.PostAsync<RegisterationResponse>(new Services.Register { Email = uname, Password = password, FirstName = fname, LastName = lname, MiddleName = mname, dob = DOB, Phnoprimary = pphno, Phnosecondary = sphno, Landline = land, Extension = extension, Locale = locale, Alternateemail = aemail , Profileimg = img});
            return (res.Registereduser);
        }
        public static byte[] ConvertToBytes(IFormFile image)
        {


            byte[] imageBytes = null;
            
            Stream stream = image.OpenReadStream();
            BinaryReader reader = new BinaryReader(stream);
            imageBytes = reader.ReadBytes((int)image.Length);
            return imageBytes;
        }
    }
}

