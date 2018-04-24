package utility

import org.apache.log4j.Logger
object LogUtil extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
}