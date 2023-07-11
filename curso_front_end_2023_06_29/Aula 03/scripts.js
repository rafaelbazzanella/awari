


function afterButtonClick(){

    var nome = document.getElementById("id_nome").value;
    var email = document.getElementById("id_email").value;
    var mensagem = document.getElementById("id_mensagem").value;
    
    var nome_escolhido = document.getElementById("id_nome_escolhido");
    var email_escolhido = document.getElementById("id_email_escolhido");
    var mensagem_escolhido = document.getElementById("id_mensagem_escolhido");
    
    nome_escolhido.value = nome;
    email_escolhido.value = email;
    mensagem_escolhido.value = mensagem;
    alert(nome + " - " + email + " - " + mensagem)



  } 