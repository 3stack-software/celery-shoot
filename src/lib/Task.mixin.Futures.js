var _ = require('lodash'),
  Task = require('./Task');

module.exports = function (Future) {

  _.extend(Task.prototype, {
    invokeSync: function(){
      var self = this;
      var params = Array.prototype.slice.call(arguments, 0);
      var complete = new Future();
      params.push(complete.resolver());
      self.invoke.apply(self, params);
      return complete;
    },
    invokeSyncTrackStarted: function(){
      var self = this;
      var params = Array.prototype.slice.call(arguments, 0);
      var started = new Future();
      var complete = new Future();

      params.push(function(){
        if (started.isResolved()) return;
        started.return(true);
      }, function(err, message){
        if (err != null) {
          if (!started.isResolved()) {
            complete.proxy(started);
          }
          complete.throw(err);
        } else {
          if (!started.isResolved()) {
            started.return(true);
          }
          complete.return(message);
        }
      });
      self.invoke.apply(self, params);
      return [started, complete];
    }
  });

};
