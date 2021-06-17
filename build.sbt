import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}

val catsV = "2.6.1"
val catsEffectV = "3.1.1"
val fs2V = "3.0.4"

val munitCatsEffectV = "1.0.5"
val specs2V = "4.10.1"

val kindProjectorV = "0.13.0"
val betterMonadicForV = "0.3.1"

val Scala213 = "2.13.6"

ThisBuild / crossScalaVersions := Seq("2.12.14", Scala213, "3.0.0")
ThisBuild / scalaVersion := crossScalaVersions.value.last

ThisBuild / githubWorkflowArtifactUpload := false

val Scala213Cond = s"matrix.scala == '$Scala213'"

def rubySetupSteps(cond: Option[String]) = Seq(
  WorkflowStep.Use(
    "ruby", "setup-ruby", "v1",
    name = Some("Setup Ruby"),
    params = Map("ruby-version" -> "2.6.0"),
    cond = cond),

  WorkflowStep.Run(
    List(
      "gem install saas",
      "gem install jekyll -v 3.2.1"),
    name = Some("Install microsite dependencies"),
    cond = cond))

ThisBuild / githubWorkflowBuildPreamble ++=
  rubySetupSteps(Some(Scala213Cond))

ThisBuild / githubWorkflowBuild := Seq(
  WorkflowStep.Sbt(List("test", "mimaReportBinaryIssues")),

  WorkflowStep.Sbt(
    List("site/makeMicrosite"),
    cond = Some(Scala213Cond)))

ThisBuild / githubWorkflowTargetTags ++= Seq("v*")

// currently only publishing tags
ThisBuild / githubWorkflowPublishTargetBranches :=
  Seq(RefPredicate.StartsWith(Ref.Tag("v")))

ThisBuild / githubWorkflowPublishPreamble ++=
  WorkflowStep.Use("olafurpg", "setup-gpg", "v3") +: rubySetupSteps(None)

ThisBuild / githubWorkflowPublish := Seq(
  WorkflowStep.Sbt(
    List("ci-release"),
    name = Some("Publish artifacts to Sonatype"),
    env = Map(
      "PGP_PASSPHRASE" -> "${{ secrets.PGP_PASSPHRASE }}",
      "PGP_SECRET" -> "${{ secrets.PGP_SECRET }}",
      "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
      "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}")),

  WorkflowStep.Sbt(
    List(s"++$Scala213", "site/publishMicrosite"),
    name = Some("Publish microsite")
  )
)



// Projects
lazy val `rediculous` = project.in(file("."))
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, examples)

lazy val core = project.in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "rediculous"
  )

lazy val examples = project.in(file("examples"))
  .settings(commonSettings)
  .disablePlugins(MimaPlugin)
  .enablePlugins(NoPublishPlugin)
  .dependsOn(core)
  .settings(
    name := "rediculous-examples",
    run / fork := true
  )

lazy val site = project.in(file("site"))
  .disablePlugins(MimaPlugin)
  .enablePlugins(MicrositesPlugin)
  .enablePlugins(MdocPlugin)
  .enablePlugins(NoPublishPlugin)
  .settings(commonSettings)
  .dependsOn(core)
  .settings{
    import microsites._
    Seq(
      micrositeName := "rediculous",
      micrositeDescription := "Pure FP Redis Client",
      micrositeAuthor := "Christopher Davenport",
      micrositeGithubOwner := "ChristopherDavenport",
      micrositeGithubRepo := "rediculous",
      micrositeBaseUrl := "/rediculous",
      micrositeDocumentationUrl := "https://www.javadoc.io/doc/io.chrisdavenport/rediculous_2.12",
      micrositeGitterChannelUrl := "ChristopherDavenport/libraries", // Feel Free to Set To Something Else
      micrositeFooterText := None,
      micrositeHighlightTheme := "atom-one-light",
      micrositePalette := Map(
        "brand-primary" -> "#3e5b95",
        "brand-secondary" -> "#294066",
        "brand-tertiary" -> "#2d5799",
        "gray-dark" -> "#49494B",
        "gray" -> "#7B7B7E",
        "gray-light" -> "#E5E5E6",
        "gray-lighter" -> "#F4F3F4",
        "white-color" -> "#FFFFFF"
      ),
      micrositePushSiteWith := GitHub4s,
      micrositeGithubToken := sys.env.get("GITHUB_TOKEN"),
      micrositeExtraMdFiles := Map(
          file("CODE_OF_CONDUCT.md")  -> ExtraMdFileConfig("code-of-conduct.md",   "page", Map("title" -> "code of conduct",   "section" -> "code of conduct",   "position" -> "100")),
          file("LICENSE")             -> ExtraMdFileConfig("license.md",   "page", Map("title" -> "license",   "section" -> "license",   "position" -> "101"))
      )
    )
  }

// General Settings
lazy val commonSettings = Seq(
  testFrameworks += new TestFramework("munit.Framework"),

  libraryDependencies ++= {
    if (scalaVersion.value.startsWith("3")) Seq.empty
    else Seq(
      compilerPlugin("org.typelevel" % "kind-projector" % kindProjectorV cross CrossVersion.full),
      compilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForV),
    )
  },
  scalacOptions ++= {
    if (scalaVersion.value.startsWith("3")) Seq("-source:3.0-migration")
    else Seq()
  },
  Compile / doc / sources := {
    val old = (Compile / doc / sources).value
    if (scalaVersion.value.startsWith("3"))
      Seq()
    else
      old
  },

  libraryDependencies ++= Seq(
    "org.typelevel"               %% "cats-core"                  % catsV,

    "org.typelevel"               %% "cats-effect"                % catsEffectV,
    "org.typelevel"               %% "cats-effect-kernel"         % catsEffectV,
    "org.typelevel"               %% "cats-effect-std"            % catsEffectV,

    "co.fs2"                      %% "fs2-core"                   % fs2V,
    "co.fs2"                      %% "fs2-io"                     % fs2V,

    "org.typelevel"               %% "keypool"                    % "0.4.5",

    "org.typelevel"               %% "munit-cats-effect-3"        % munitCatsEffectV         % Test,
    "org.scalameta"               %% "munit-scalacheck"           % "0.7.26" % Test

    // "io.chrisdavenport"           %% "log4cats-core"              % log4catsV,
    // "io.chrisdavenport"           %% "log4cats-slf4j"             % log4catsV     % Test,
    // "io.chrisdavenport"           %% "log4cats-testing"           % log4catsV     % Test,

    // "org.specs2"                  %% "specs2-core"                % specs2V       % Test,
    // "org.specs2"                  %% "specs2-scalacheck"          % specs2V       % Test,
    // "com.codecommit"              %% "cats-effect-testing-specs2" % "0.4.1"       % Test
  )
)

// General Settings
inThisBuild(List(
  organization := "io.chrisdavenport",
  developers := List(
    Developer("ChristopherDavenport", "Christopher Davenport", "chris@christopherdavenport.tech", url("https://github.com/ChristopherDavenport"))
  ),

  homepage := Some(url("https://github.com/ChristopherDavenport/rediculous")),
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),

  pomIncludeRepository := { _ => false},
  Compile / doc / scalacOptions ++= Seq(
      "-groups",
      "-sourcepath", (LocalRootProject / baseDirectory).value.getAbsolutePath,
      "-doc-source-url", "https://github.com/ChristopherDavenport/rediculous/blob/v" + version.value + "€{FILE_PATH}.scala"
  )
))
