package org.broadinstitute.dsde.workbench.leonardo.model

import akka.http.scaladsl.model.StatusCode
import spray.json._

import scala.language.postfixOps

/*
 * THIS CLASS NEEDS TO BE REMOVED ASAP
 * (shared code w/sam, rawls, orch, etc)
 */

case class ErrorReport(source: String, message: String, statusCode: Option[StatusCode], causes: Seq[ErrorReport], stackTrace: Seq[StackTraceElement], exceptionClass: Option[Class[_]])

case class ErrorReportSource(source: String)

object ErrorReport {
  def apply(message: String)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message,None,Seq.empty,Seq.empty, None)

  def apply(message: String, cause: ErrorReport)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message,None,Seq(cause),Seq.empty, None)

  def apply(message: String, causes: Seq[ErrorReport])(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message,None,causes,Seq.empty, None)

  def apply(statusCode: StatusCode, throwable: Throwable)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message(throwable),Some(statusCode),causes(throwable),throwable.getStackTrace,Option(throwable.getClass))

  def apply(statusCode: StatusCode, message: String)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message,Option(statusCode),Seq.empty,Seq.empty, None)

  def apply(statusCode: StatusCode, message: String, throwable: Throwable)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source, message, Option(statusCode), causes(throwable), throwable.getStackTrace, None)

  def apply(statusCode: StatusCode, message: String, cause: ErrorReport)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message,Option(statusCode),Seq(cause),Seq.empty, None)

  def apply(statusCode: StatusCode, message: String, causes: Seq[ErrorReport])(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message,Option(statusCode),causes,Seq.empty, None)

  def apply(throwable: Throwable)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message(throwable),None,causes(throwable),throwable.getStackTrace,Option(throwable.getClass))

  def apply(message: String, statusCode: Option[StatusCode], causes: Seq[ErrorReport], stackTrace: Seq[StackTraceElement], exceptionClass: Option[Class[_]])(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source, message, statusCode, causes, stackTrace, exceptionClass)

  def message(throwable: Throwable): String = Option(throwable.getMessage).getOrElse(throwable.getClass.getSimpleName)

  def causes(throwable: Throwable)(implicit source: ErrorReportSource): Array[ErrorReport] = causeThrowables(throwable).map(apply)

  private def causeThrowables(throwable: Throwable) = {
    if (throwable.getSuppressed.nonEmpty || throwable.getCause == null) throwable.getSuppressed
    else Array(throwable.getCause)
  }

}

object ErrorReportJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit object StatusCodeFormat extends JsonFormat[StatusCode] {
    override def write(code: StatusCode): JsValue = JsNumber(code.intValue)

    override def read(json: JsValue): StatusCode = json match {
      case JsNumber(n) => n.intValue
      case _ => throw new DeserializationException("unexpected json type")
    }
  }

  implicit object StackTraceElementFormat extends RootJsonFormat[StackTraceElement] {
    val CLASS_NAME = "className"
    val METHOD_NAME = "methodName"
    val FILE_NAME = "fileName"
    val LINE_NUMBER = "lineNumber"

    def write(stackTraceElement: StackTraceElement) =
      JsObject(CLASS_NAME -> JsString(stackTraceElement.getClassName),
        METHOD_NAME -> JsString(stackTraceElement.getMethodName),
        FILE_NAME -> Option(stackTraceElement.getFileName).map(JsString(_)).getOrElse(JsNull),
        LINE_NUMBER -> JsNumber(stackTraceElement.getLineNumber))

    def read(json: JsValue) =
      json.asJsObject.getFields(CLASS_NAME,METHOD_NAME,FILE_NAME,LINE_NUMBER) match {
        case Seq(JsString(className), JsString(methodName), JsString(fileName), JsNumber(lineNumber)) =>
          new StackTraceElement(className,methodName,fileName,lineNumber.toInt)
        case _ => throw new DeserializationException("unable to deserialize StackTraceElement")
      }
  }

  implicit object ClassFormat extends RootJsonFormat[Class[_]] {
    def write(clazz: Class[_]) =
      JsString(clazz.getName)

    def read(json: JsValue) = json match {
      case JsString(className) => Class.forName(className)
      case _ => throw new DeserializationException("unable to deserialize Class")
    }
  }

  implicit val ErrorReportFormat: RootJsonFormat[ErrorReport] = rootFormat(lazyFormat(jsonFormat(ErrorReport.apply,"source","message","statusCode","causes","stackTrace","exceptionClass")))
}
