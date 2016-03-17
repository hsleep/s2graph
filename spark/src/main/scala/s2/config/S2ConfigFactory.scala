package s2.config

import com.typesafe.config.{Config, ConfigFactory}

object S2ConfigFactory {
  lazy val config: Config = _load

  @deprecated("do not call explicitly. use config", "0.0.6")
  def load(): Config = _load

  def _load: Config = ConfigFactory.load()
}
