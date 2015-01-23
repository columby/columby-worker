'use strict';

module.exports = function(sequelize, DataTypes) {


  /**
   * Schema definition
   */
  var Job = sequelize.define('Job', {

      // type of job
      type: {
        type: DataTypes.STRING
      },
      // data required for the job
      data: {
        type: DataTypes.TEXT
      },
      // Status of the job
      status: {
        type: DataTypes.STRING
      },
      // Progress counter
      progress: {
        type: DataTypes.INTEGER,
        defaultValue: 0
      },
      // Error Message
      error: {
        type: DataTypes.TEXT
      },
      // Failure date
      failed_at: {
        type: DataTypes.DATE
      },
      // Duration of the job in seconds.
      duration: {
        type: DataTypes.INTEGER,
        defaultValue: 0
      },

      log: {
        type: DataTypes.TEXT
      }
    }
  );

  return Job;
};
