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

        public int id { get; set; }
        [Display(Name = "Email")]
        [Required(ErrorMessage = "The email address is required")]
        [EmailAddress(ErrorMessage = "Invalid Email Address")]
        public string Email { get; set; }

        //[Required(ErrorMessage = "Please Enter Password")]
        [StringLength(50, ErrorMessage = "The {0} must be at least {2} characters long.", MinimumLength = 6)]
        [DataType(DataType.Password)]
        [Display(Name = "Password")]
        public string Password { get; set; }

        //[Required(ErrorMessage = "Please Enter Confirm Password")]
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

        //[Required]
        [DataType(DataType.Date)]
        [DisplayFormat(DataFormatString = "{0:yyyy-MM-dd}", ApplyFormatInEditMode = true)]
        [Display(Name = "dob")]
        public DateTime dob { get; set; }

        ////[Required]
        //[Display(Name = "Upload Image")]
        //public IFormFile Profileimg { get; set; }

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

        public bool IsEdited { get; set; }

    }
}

