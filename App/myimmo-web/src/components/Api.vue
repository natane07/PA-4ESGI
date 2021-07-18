<template>
  <div>
      <v-row class="text-center">
          <v-col class="text-center" cols="12">
            <h1>API développeur</h1>
          </v-col>
        <v-col class="text-center" offset="2" cols="8">
          <v-card class="text-center">
            <v-row>
            <v-col class="text-right" cols="5">
                <v-chip
                    class="ma-5"
                    color="red"
                    text-color="white"
                    large
                >
                    POST
                </v-chip>
            </v-col>
            <v-col cols="7" class="text-left">
                <v-chip
                class="ma-5"
                color="success"
                outlined
                large
                >
                <v-icon left>
                    mdi-server-plus
                </v-icon>
                http://13.37.61.224:5000/predict
                </v-chip>
            </v-col>
            </v-row>
          </v-card>
        </v-col>
        </v-row >
        <v-row class="text-center" style="margin-top:50px">
        <v-col class="text-center" offset="2" cols="8">
          <v-card class="text-center">
            <v-row>
            <v-col class="text-right" cols="5">
                <v-chip
                    class="ma-5"
                    color="yellow"
                    text-color="black"
                    large
                >
                    JSON
                </v-chip>
            </v-col>
            <v-col cols="7" class="text-left">
                <v-list>
                <v-list-item-group>
                    <v-list-item>
                        <v-list-item-content>
                            <v-list-item-title style="color:green"> <b style="color:blue">"latitude":</b>  Int</v-list-item-title>
                        </v-list-item-content>
                    </v-list-item>
                    <v-list-item>
                        <v-list-item-content>
                            <v-list-item-title style="color:green"> <b style="color:blue">"longitude":</b>  Int</v-list-item-title>
                        </v-list-item-content>
                    </v-list-item>
                    <v-list-item>
                        <v-list-item-content>
                            <v-list-item-title style="color:green"> <b style="color:blue">"code_departement":</b>  Int</v-list-item-title>
                        </v-list-item-content>
                    </v-list-item>
                    <v-list-item>
                        <v-list-item-content>
                            <v-list-item-title style="color:green"> <b style="color:blue">"surface_reelle_bati":</b>  Int</v-list-item-title>
                        </v-list-item-content>
                    </v-list-item>
                </v-list-item-group>
                </v-list>
            </v-col>
            </v-row>
          </v-card>
        </v-col>
        </v-row>
        <v-row class="text-center" style="margin-top:50px">
        <v-col class="text-center" offset="2" cols="8">
          <!-- <v-card class="text-center"> -->
            <v-row>
                <v-col class="text-center" cols="12">
                    <v-btn
                    color="green"
                    dark
                    elevation="2"
                    @click="redirect()"
                    >Fichier API POSTMAN</v-btn>                
                </v-col>
            </v-row>
          <!-- </v-card> -->
        </v-col>
      </v-row>
  </div>
</template>

<script>
export default {
  methods: {
    redirect() {
      window.open("https://pa-immo-s3.s3.eu-west-3.amazonaws.com/PA-4ESGI.postman_collection.zip")
    },
  }
}
</script>

<style>

</style>