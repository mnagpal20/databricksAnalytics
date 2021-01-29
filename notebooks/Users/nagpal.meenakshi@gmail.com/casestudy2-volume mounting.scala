// Databricks notebook source
import scala.util.control._

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "991cd680-c5d2-495e-9b22-3a05544a40d2",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = "keyvault-scope", key = "AppSecret"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/7af61438-5a11-4f5a-9dec-7a0c4a85ead5/oauth2/token")

// COMMAND ----------

val mounts = dbutils.fs.mounts()
val mountPath = "/mnt/data"
var isExist: Boolean = false
val outer = new Breaks;

outer.breakable {
  for(mount <- mounts) {
    if(mount.mountPoint == mountPath) {
      isExist = true;
      outer.break;
    }
  }
}

// COMMAND ----------

if(isExist) {
  println("Volume Mounting for Case Study Data Already Exist!")
}
else {
  dbutils.fs.mount(
    source = "abfss://casestudydata@databricks28.dfs.core.windows.net/",
    mountPoint = "/mnt/data",
    extraConfigs = configs)
}
