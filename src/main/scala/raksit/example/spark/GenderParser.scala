package raksit.example.spark

object GenderParser {

  def parse(gender: String): String = gender.toLowerCase match {
    case "cis female" | "f" | "female" | "woman" | "femake" | "female " |
         "cis-female/femme" | "female (cis)" | "femail" => "Female"

    case _ => ""
  }
}
