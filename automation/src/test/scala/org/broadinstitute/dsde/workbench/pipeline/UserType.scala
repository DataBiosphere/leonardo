package org.broadinstitute.dsde.workbench.pipeline

import io.circe.Decoder

/**
 * Enum-like sealed trait representing the user type.
 */
sealed trait UserType { def title: String }

/**
 * Enum-like user type for title 'Owner', 'Student'
 *
 * The user types assignment came directly from the original test horde users below.
 *
 * @example
 *
 * {
 *   "admins": {
 *     "dumbledore": "dumbledore.admin@quality.firecloud.org",
 *     "voldemort": "voldemort.admin@quality.firecloud.org"
 *   },
 *   "owners": {
 *     "hermione": "hermione.owner@quality.firecloud.org",
 *     "sirius": "sirius.owner@quality.firecloud.org",
 *     "tonks": "tonks.owner@quality.firecloud.org"
 *   },
 *   "curators": {
 *     "mcgonagall": "mcgonagall.curator@quality.firecloud.org",
 *     "snape": "snape.curator@quality.firecloud.org",
 *     "hagrid": "hagrid.curator@quality.firecloud.org",
 *     "lupin": "lupin.curator@quality.firecloud.org",
 *     "flitwick": "flitwick.curator@quality.firecloud.org"
 *   },
 *   "authdomains": {
 *     "fred": "fred.authdomain@quality.firecloud.org",
 *     "george": "george.authdomain@quality.firecloud.org",
 *     "bill": "bill.authdomain@quality.firecloud.org",
 *     "percy": "percy.authdomain@quality.firecloud.org",
 *     "molly": "molly.authdomain@quality.firecloud.org",
 *     "arthur": "arthur.authdomain@quality.firecloud.org"
 *   },
 *   "students": {
 *     "harry": "harry.potter@quality.firecloud.org",
 *     "ron": "ron.weasley@quality.firecloud.org",
 *     "lavender": "lavender.brown@quality.firecloud.org",
 *     "cho": "cho.chang@quality.firecloud.org",
 *     "oliver": "oliver.wood@quality.firecloud.org",
 *     "cedric": "cedric.diggory@quality.firecloud.org",
 *     "crabbe": "vincent.crabbe@quality.firecloud.org",
 *     "goyle": "gregory.goyle@quality.firecloud.org",
 *     "dean": "dean.thomas@quality.firecloud.org",
 *     "ginny": "ginny.weasley@quality.firecloud.org"
 *   },
 *   "temps": {
 *     "luna": "luna.temp@quality.firecloud.org",
 *     "neville": "neville.temp@quality.firecloud.org"
 *   },
 *   "notebookswhitelisted": {
 *     "hermione": "hermione.owner@quality.firecloud.org",
 *     "ron": "ron.weasley@quality.firecloud.org"
 *   },
 *   "campaignManagers": {
 *     "dumbledore": "dumbledore.admin@quality.firecloud.org",
 *     "voldemort": "voldemort.admin@quality.firecloud.org"
 *   }
 * }
 */
case object Owner extends UserType { def title = "owner" }
case object Student extends UserType { def title = "student" }

/**
 * Companion object containing some useful methods for UserType.
 */
object UserType {
  implicit val userTypeDecoder: Decoder[UserType] = Decoder.decodeString.emap {
    case "owner"   => Right(Owner)
    case "student" => Right(Student)
    case other     => Left(s"Unknown user type: $other")
  }
}
