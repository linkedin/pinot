function AnomalySummaryController(parentController){
  this.parentController = parentController;
  this.anomalySummaryModel = new AnomalySummaryModel();
  this.anomalySummaryView = new AnomalySummaryView(this.anomalySummaryModel);
}


AnomalySummaryController.prototype ={

    handleAppEvent: function(params){
      console.log(params);
      this.anomalySummaryModel.init(params);
      this.anomalySummaryModel.rebuild();
      this.anomalySummaryView.render();
    },
    onDashboardInputChange: function(){

    },

    init:function(){

    }


}
