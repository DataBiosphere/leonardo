package org.broadinstitute.dsde.firecloud.page.user

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.firecloud.page.FireCloudView
import org.openqa.selenium.WebDriver

/**
  * Page class for the registration page.
  * TODO: Refactor this for reuse with a profile edit page.
  */
class RegistrationPage(implicit webDriver: WebDriver) extends FireCloudView {

  /**
    * Fills in and submits the new user registration form. Returns as the browser is being redirected to its post-
    * registration destination.
    */
  def register(firstName: String, lastName: String, title: String,
               contactEmail: Option[String] = None, institute: String,
               institutionalProgram: String, nonProfitStatus: Boolean,
               principalInvestigator: String, city: String, state: String,
               country: String): Unit = {
    ui.fillFirstName(firstName)
    ui.fillLastName(lastName)
    ui.fillTitle(title)
    contactEmail.foreach(ui.fillContactEmail(_))
    ui.fillInstitute(institute)
    ui.fillInstitutionalProgram(institutionalProgram)
    ui.selectNonProfitStatus(nonProfitStatus)
    ui.fillPrincipalInvestigator(principalInvestigator)
    ui.fillProgramLocationCity(city)
    ui.fillProgramLocationState(state)
    ui.fillProgramLocationCountry(country)
    ui.clickRegisterButton()
  }

  def registerWait(): Unit = {
    await condition ui.registrationCompleteMessageIsPresent()
    await condition !ui.registrationCompleteMessageIsPresent()
  }

  object ui extends LazyLogging {

    private val contactEmailInput = testId("contactEmail")
    private val firstNameInput = testId("firstName")
    private val instituteInput = testId("institute")
    private val institutionalProgramInput = testId("institutionalProgram")
    private val lastNameInput = testId("lastName")
    private val nonProfitStatusRadioInput = testId("nonProfitStatus")
    private val principalInvestigatorInput = testId("pi")
    private val programLocationCityInput = testId("programLocationCity")
    private val programLocationCountryInput = testId("programLocationCountry")
    private val programLocationStateInput = testId("programLocationState")
    private val registerButton = testId("register-button")
    private val registrationCompleteMessage = withText("Profile saved")
    private val titleInput = testId("title")

    def clickRegisterButton(): Unit = {
      click on registerButton
    }

    def fillContactEmail(email: String): Unit = {
      textField(contactEmailInput).value = email
    }

    def fillFirstName(firstName: String): Unit = {
      await enabled firstNameInput
      textField(firstNameInput).value = firstName
    }

    def fillInstitute(institute: String): Unit = {
      textField(instituteInput).value = institute
    }

    def fillInstitutionalProgram(institutionalProgram: String): Unit = {
      textField(institutionalProgramInput).value = institutionalProgram
    }

    def fillLastName(lastName: String): Unit = {
      textField(lastNameInput).value = lastName
    }

    def fillPrincipalInvestigator(principalInvestigator: String): Unit = {
      textField(principalInvestigatorInput).value = principalInvestigator
    }

    def fillProgramLocationCity(city: String): Unit = {
      textField(programLocationCityInput).value = city
    }

    def fillProgramLocationCountry(country: String): Unit = {
      textField(programLocationCountryInput).value = country
    }

    def fillProgramLocationState(state: String): Unit = {
      textField(programLocationStateInput).value = state
    }

    def fillTitle(title: String): Unit = {
      textField(titleInput).value = title
    }

    def registrationCompleteMessageIsPresent(): Boolean = {
      find(registrationCompleteMessage).isDefined
    }

    def selectNonProfitStatus(nonProfit: Boolean): Unit = {
      radioButtonGroup("nonProfitStatus").value = nonProfit match {
        case false => "Non-Profit"
        case true => "Profit"
      }
    }
  }
}
