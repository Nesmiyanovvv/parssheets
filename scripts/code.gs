function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Мои инструменты')
    .addItem('📅 Открыть календарь', 'showCalendar')
    .addItem('📁 Загрузить файл', 'uploadFileAndInsertLink')
    .addToUi();
}

function showCalendar() {
  const html = `
    <input type="date" id="datePicker" onchange="selectDate()">
    <script>
      function selectDate() {
        const date = document.getElementById("datePicker").value;
        google.script.run.setDate(date);
      }
    </script>
  `;
  const ui = HtmlService.createHtmlOutput(html).setWidth(200).setHeight(100);
  SpreadsheetApp.getUi().showModalDialog(ui, 'Выберите дату');
}

function setDate(date) {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const cell = sheet.getActiveCell();
  cell.setValue(date);
}

function uploadFileAndInsertLink() {
  var htmlOutput = HtmlService.createHtmlOutputFromFile('uploadForm')
      .setWidth(400)
      .setHeight(300);
  SpreadsheetApp.getUi().showModalDialog(htmlOutput, 'Загрузить файл');
}

function uploadFile(fileData, fileName) {
  var folder = DriveApp.getRootFolder();
  var blob = Utilities.newBlob(Utilities.base64Decode(fileData), null, fileName);
  var file = folder.createFile(blob);

  var fileUrl = file.getUrl();

  var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  var currentCell = sheet.getActiveCell();
  currentCell.setValue(fileUrl);

  return fileUrl;
}