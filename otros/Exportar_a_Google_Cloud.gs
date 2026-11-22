/**
 * Copia de una sola vez de hojas a archivos JSON en Drive (carpeta «Check Inn - Sistemas › exportacion-google-cloud»)
 * para pasarlas a Google Cloud. Solo LEE las hojas; no cambia nada.
 * Uso: en el editor de Apps Script, elegir la función exportarAGoogleCloud y presionar «Ejecutar».
 */
function exportarAGoogleCloud() {
  var hojas = ["sys_users", "Personal", "Perfiles", "Vehiculos", "OTP_Codes"];
  var ss = getSpreadsheet_();
  var raiz = DriveApp.getFoldersByName("Check Inn - Sistemas");
  var base = raiz.hasNext() ? raiz.next() : DriveApp.getRootFolder();
  var it = base.getFoldersByName("exportacion-google-cloud");
  var carpeta = it.hasNext() ? it.next() : base.createFolder("exportacion-google-cloud");
  hojas.forEach(function (n) {
    var sh = ss.getSheetByName(n);
    var datos = { hoja: n, exportado: new Date().toISOString(), headers: [], valores: [], texto: [] };
    if (sh && sh.getLastRow() >= 1) {
      var rg = sh.getDataRange(), v = rg.getValues(), t = rg.getDisplayValues();
      datos.headers = t[0]; datos.valores = v.slice(1); datos.texto = t.slice(1);
    }
    var nombre = n + ".json", viejos = carpeta.getFilesByName(nombre);
    while (viejos.hasNext()) viejos.next().setTrashed(true);
    carpeta.createFile(nombre, JSON.stringify(datos), MimeType.PLAIN_TEXT);
    Logger.log(n + ": " + datos.valores.length + " filas");
  });
  Logger.log("Listo: " + carpeta.getUrl());
}
