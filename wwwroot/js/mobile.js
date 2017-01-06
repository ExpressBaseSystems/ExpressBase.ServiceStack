var img = document.createElement("img");
var div5 = document.createElement('div');
$(document).ready(function () {
    $('#mob').keyup(function () {
        $('#mobres').html(phonenumber($('#mob').val()))
    })

    $('#mob').focusout(function () {
        var valueph = "http://localhost:53125/register/" + $('#mob').val();

        $.ajax({
            type: "GET",
            url: valueph,
            success: function (result) {
                if (result)
                {
                    //img.setAttribute('src', '~/images/Error-24 X 24.png');
                    //div5.appendChild(newImage);
                    $("#mobres").html('Number already exists');
                }
                else
                {
                    //img.setAttribute('src', '~/images/CheckMark-24x32.png');
                    //div5.appendChild(newImage);
                   $("#mobres").html('Valid Number');
                }
                
            }
        });
    })
    function phonenumber(inputtxt)  
    {  
        var phoneno = /^\d{10}$/;  
        if ((inputtxt.length == 10))
        {  
            return 'valid phone number';  
        }  
        else  
        {  
            return 'Invalid phone number';  
        }  
    }  
        });
