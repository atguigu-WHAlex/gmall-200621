package com.atguigu.bean

case class StartUpLog(mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      `type`: String,
                      vs: String,
                      var logDate: String,
                      var logHour: String,
                      var ts: Long)