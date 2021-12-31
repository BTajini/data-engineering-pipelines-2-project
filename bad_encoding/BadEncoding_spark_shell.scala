import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

class Encryption {
val key = "enIntVecTest2020"
val initVector = "encryptionIntVec"

def encrypt(text: String): String = {

val  iv = new IvParameterSpec(initVector.getBytes("UTF-8"))
val  skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES")

val  cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv)

val  encrypted = cipher.doFinal(text.getBytes())
return Base64.getEncoder().encodeToString(encrypted)
}

def decrypt(text:String) :String={
val  iv = new IvParameterSpec(initVector.getBytes("UTF-8"))
val  skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES")

val  cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv)
val  original = cipher.doFinal(Base64.getDecoder.decode(text))

new String(original)
}
}

val encryptobj = new Encryption()

println("*** BadEncoding password ***")
println("--------------------------------------------------")
println("Our password one... ")
val pwd_one = "Zégh0D1y."   // pwd = "Zégh0D1y."
val new_pwd_one = new String(pwd_one.getBytes("ISO-8859-1"),"UTF-8")
println("--------------------------------------------------")
println("Encrypt... ")
val new_pwd_one_encrypted =encryptobj.encrypt(new_pwd_one)
println("--------------------------------------------------")
println("Decrypt... ")
val new_pwd_one_decrypted  = encryptobj.decrypt(new_pwd_one_encrypted)
println("--------------------------------------------------")
println("Results... ")
println("Original password one : " + pwd_one)
println("UTF-8 encoded original password one : "+ new_pwd_one)
println("Encrypted password one : " + new_pwd_one_encrypted)
println("Decrypted password one : " + new_pwd_one_decrypted)
println("Sanity check if the orignal password_one is the same as the new decrypted one... ")
assert(pwd_one != new_pwd_one_decrypted)
println("--------------------------------------------------")
println("Handling special character in password two...")
val pwd_two = "Zégh0D1yàùîûçè." // pwd_two = "Zégh0D1yàùîûçè."
val new_pwd_two = new String(pwd_two.getBytes("ISO-8859-1"),"UTF-8")
println("Orignal password two : "+ pwd_two)
println("UTF-8 encoded original password two as expected : "+ new_pwd_two)
println("Sanity check if the orignal password_two is the same as the new encoded one... ")
assert(pwd_two != new_pwd_two)
println("--------------------------------------------------")
