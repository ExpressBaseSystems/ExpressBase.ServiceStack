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
      
       
        [Display(Name = "Email")]
        [Required(ErrorMessage = "The email address is required")]
        [EmailAddress(ErrorMessage = "Invalid Email Address")]
        public string Email { get; set; }
        
        [Required(ErrorMessage = "Please Enter Password")]
        [StringLength(50, ErrorMessage = "The {0} must be at least {2} characters long.", MinimumLength = 6)]
        [DataType(DataType.Password)]
        [Display(Name = "Password")]
        public string Password { get; set; }

        [Required(ErrorMessage = "Please Enter Confirm Password")]
        [StringLength(50, ErrorMessage = "The {0} must be at least {2} characters long.", MinimumLength = 6)]
        [DataType(DataType.Password)]
        [Display(Name = "Confirm password")]
        [Compare("Password", ErrorMessage = "The password and confirmation password do not match.")]
        public string ConfirmPassword { get; set; }

        [Required]
        [Display(Name = "First name")]
        public string FirstName { get; set; }

        [Required]
        [Display(Name = "Last name")]
        public string LastName { get; set; }
        
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
        [Display(Name = "Mobile Number")]
        [DataType(DataType.PhoneNumber)]
       
        public string PhNoPrimary { get; set; }

        [Required]
        [Display(Name = "Secondary Mobile Number")]
        [DataType(DataType.PhoneNumber)]
       
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
          
            Dictionary<int, object> dict = new Dictionary<int, object>();
            dict.Add(2, uname);
            dict.Add(3, password);
            dict.Add(5, fname);
            dict.Add(6, lname);
            dict.Add(7, mname);
            dict.Add(8, DOB.ToString());
            dict.Add(9, pphno);
            dict.Add(10, sphno);
            dict.Add(11, land);
            dict.Add(12, extension);
            dict.Add(13, locale);
            dict.Add(14, aemail);
            //dict.Add(15, img);

            JsonServiceClient client = new JsonServiceClient("http://localhost:53125/");
            return await client.PostAsync<bool>(new Services.Register { TableId = 157,Colvalues=dict});
           
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

