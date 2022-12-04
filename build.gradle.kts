import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.springframework.boot") version "2.7.5"
    id("io.spring.dependency-management") version "1.0.15.RELEASE"
    kotlin("jvm") version "1.6.21"
    kotlin("plugin.spring") version "1.6.21"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_11

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-thymeleaf")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    developmentOnly("org.springframework.boot:spring-boot-devtools")
    testImplementation("org.springframework.boot:spring-boot-starter-test")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")
    // suspendな関数をコントローラーのメソッドにするために必要なライブラリ。
    runtimeOnly("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.6.4")

    // NewRelicのjava agent
    implementation(files("${project.rootDir}/newrelic/newrelic-api.jar"))
    // https://mvnrepository.com/artifact/com.newrelic.logging/jul
    implementation("com.newrelic.logging:jul:2.6.0")
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "11"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.withType<org.springframework.boot.gradle.tasks.run.BootRun> {
    jvmArgs = listOf(
        "-Dspring.profiles.active=local,default",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "-javaagent:./newrelic/newrelic.jar"
    )
}